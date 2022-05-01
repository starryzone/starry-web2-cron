import fetch from "node-fetch"
import { config } from "dotenv"
import knex from 'knex'
import { Logtail } from "@logtail/node";

// Set up dotenv for environment variables
config({ path: '.env' });

// Set up logging
const LOGTAIL_TOKEN = process.env.LOGTAIL_TOKEN
const logtail = new Logtail(LOGTAIL_TOKEN);

// All other env vars
const STARRY_BACKEND = process.env.STARRY_BACKEND
const DB_HOSTIP = process.env.DB_HOSTIP
const PORT = process.env.PORT
const DB_HOSTPORT = process.env.DB_HOSTPORT
const DB_NAME = process.env.DB_NAME
const DB_USER = process.env.DB_USER
const DB_PASS = process.env.DB_PASS
const DB_TABLENAME_MEMBERS = process.env.DB_TABLENAME_MEMBERS
const DB_TABLENAME_SYNC = process.env.DB_TABLENAME_SYNC
const DB_TABLENAME_SYNC_LOGS = process.env.DB_TABLENAME_SYNC_LOGS
const TIMEOUT = process.env.TIMEOUT

// Set up guild(s) to update
const GUILDS_TO_UPDATE = process.env.GUILDS_TO_UPDATE
// This one needs to be mutable
let guildsToUpdate = JSON.parse(GUILDS_TO_UPDATE)

// This will keep track of how many members were updated
let membersPerGuild = {}

const enableSSL = !['localhost', '127.0.0.1'].includes(DB_HOSTIP);

const db = knex({
  client: 'pg',
  connection: {
    user: DB_USER,
    password: DB_PASS,
    database: DB_NAME,
    host: DB_HOSTIP,
    port: DB_HOSTPORT,
    ssl: enableSSL
  },
  pool: {
    max: 5,
    min: 5,
    acquireTimeoutMillis: 60000,
    createTimeoutMillis: 30000,
    idleTimeoutMillis: 600000,
    createRetryIntervalMillis: 200,
  }
});

// Makes POST request to backend to sync a Discord member from a guild
const updateGuildUser = async (guildId, discordUserId) => {
  // Log intention
  await logtail.info('calling token rule info', {
    event: 'track-user-flow',
    time_relative: 'before',
    predicate: 'calling /token-rule-info',
    data: {
      guildId,
      discordUserId
    }
  })

  // Make request to update this member from this guild
  const requestOptions = {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ discordUserId, guildId })
  }
  const res = await fetch(`${STARRY_BACKEND}:${PORT}/token-rule-info`, requestOptions)

  switch (res.status) {
    case 200:
      // Expected behavior
      await logtail.info('calling token rule info', {
        event: 'track-user-flow',
        time_relative: 'after',
        predicate: 'calling /token-rule-info',
        data: {
          guildId,
          discordUserId,
          res: {
            status: res.status,
            statusText: res.statusText,
            json: await res.json()
          }
        }
      })
      break
    case 400:
      await logtail.error('400 status', {
        event: 'received 400 status',
        res: {
          status: res.status,
          statusText: res.statusText,
          discordUserId,
          guildId
        }
      })
      break;
    default:
      // Alert of oddness
      await logtail.error('unexpected status', {
        event: 'received unexpected status',
        res: {
          status: res.status,
          statusText: res.statusText,
        }
      })
  }
}

// Update or insert database row for this guild,
// indicating it is beginning a sync
const setBeginUpdate = async (guildId) => {
  const rowExists = await db(DB_TABLENAME_SYNC)
    .where('discord_guild_id', guildId)
    .count('discord_guild_id')
  // Should be 0 or 1, depending on whether it's been added
  const numRows = parseInt(rowExists[0]['count'])
  if (numRows === 0) {
    // Insert
    await db(DB_TABLENAME_SYNC)
      .insert({
        discord_guild_id: guildId,
        began_update: 'now()',
        finished_update: null
      })
  } else {
    await db(DB_TABLENAME_SYNC)
      .update({
        began_update: 'now()',
        finished_update: null
      })
      .where('discord_guild_id', guildId)
  }
}

// Add more permanent row outside of logging
const AddSyncLog = async (guildId, memberCount) => {
  await db(DB_TABLENAME_SYNC_LOGS)
    .insert({
      discord_guild_id: guildId,
      members_updated: memberCount
    })
}

// Entrypoint
// This is a recursive function based on a recommendation here:
// https://developer.mozilla.org/en-US/docs/Web/API/setInterval#ensure_that_execution_duration_is_shorter_than_interval_frequency
const cronMe = async (guildId, members = null) => {
  setTimeout(async () => {
    if (members === null) {
      // Load members directly from database
      // (this happens the first time a guild begins an update)
      members = await db(DB_TABLENAME_MEMBERS)
        .where('discord_guild_id', guildId)
        .whereNotNull('cosmos_address')
        .select(['discord_account_id', 'cosmos_address']).distinct('discord_account_id')
      // Keep track of how many members we'll have synced
      membersPerGuild[guildId] = members.length
      await setBeginUpdate(guildId)
    }
    // If there are no members to update for this guild
    if (members.length === 0) {
      // Update
      await db(DB_TABLENAME_SYNC)
        .update({finished_update: 'now()'})
        .where('discord_guild_id', guildId)
        // Increase the times_updated by 1
        .increment('times_updated', 1)
      // Recurse if there are more guilds to update
      const guildMemberCount = membersPerGuild[guildId]
      if (guildsToUpdate.length) {
        await logtail.info(`Finished updating guild ${guildId} with ${guildMemberCount} members.`)
        await AddSyncLog(guildId, guildMemberCount)
        cronMe(guildsToUpdate.pop())
      } else {
        await logtail.info(`Finished updating the final guild: ${guildId} with ${guildMemberCount} members.`)
        await AddSyncLog(guildId, guildMemberCount)
        process.exit()
      }
    } else {
      const member = members.pop()
      try {
        // Call endpoint that will update user's roles
        const discordAccountId = member['discord_account_id']
        await logtail.info(`doing something with member ${discordAccountId} for guild ${guildId}`)
        await updateGuildUser(guildId, discordAccountId)
      } catch (e) {
        await logtail.error('Failure updating guild user', {
          event: 'calling updateGuildUser',
          res: {
            error: JSON.stringify(e),
          }
        })
      }
      cronMe(guildId, members)
    }
  }, TIMEOUT);
}

if (guildsToUpdate && guildsToUpdate.length) {
  cronMe(guildsToUpdate.pop()).then(() => logtail.info('Beginning…'))
} else {
  console.log('No guilds to update, please add the proper environment variable to ' +
    'look like this:\nGUILDS_TO_UPDATE=["123821047347744788","808885156490133514"]')
}
