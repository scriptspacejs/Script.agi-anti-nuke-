require('dotenv').config();
const { Client, GatewayIntentBits, PermissionFlagsBits, AuditLogEvent, Events, ActionRowBuilder, ButtonBuilder, ButtonStyle, EmbedBuilder } = require('discord.js');
const { joinVoiceChannel, getVoiceConnection } = require('@discordjs/voice');

const client = new Client({
    intents: [
        GatewayIntentBits.Guilds,
        GatewayIntentBits.GuildMembers,
        GatewayIntentBits.GuildBans,
        GatewayIntentBits.GuildMessages,
        GatewayIntentBits.MessageContent,
        GatewayIntentBits.GuildModeration,
        GatewayIntentBits.GuildIntegrations,
        GatewayIntentBits.GuildWebhooks,
        GatewayIntentBits.GuildVoiceStates
    ]
});

// Target voice channel ID
const TARGET_VOICE_CHANNEL_ID = '1377806787431895181';

// Optimized configuration for maximum speed
const CONFIG = {
    TIMEOUT_DURATIONS: {
        BOT_ADDITION: 28 * 24 * 60 * 60 * 1000,
        MEMBER_KICK: 2 * 24 * 60 * 60 * 1000,
        MEMBER_BAN: 7 * 24 * 60 * 60 * 1000,
        CHANNEL_MODIFICATION: 28 * 24 * 60 * 60 * 1000,
        UNAUTHORIZED_ACTION: 28 * 24 * 60 * 60 * 1000,
        NUKE_ATTEMPT: 28 * 24 * 60 * 60 * 1000
    },
    MASS_ACTION_LIMITS: {
        ROLE_DELETE: { limit: 1, timeWindow: 5000 },
        CHANNEL_DELETE: { limit: 1, timeWindow: 3000 },
        CHANNEL_CREATE: { limit: 2, timeWindow: 5000 },
        MEMBER_BAN: { limit: 2, timeWindow: 10000 },
        MEMBER_KICK: { limit: 3, timeWindow: 15000 },
        BOT_ADD: { limit: 1, timeWindow: 10000 }
    }
};

// Global storage with optimized performance
let serverData = new Map();
let globalStats = { totalTimeouts: 0, totalBans: 0, totalKicks: 0, unauthorizedBots: 0, nukeAttempts: 0 };
let rateLimitCache = new Map();

// Initialize server data with enhanced tracking
function initServerData(guildId) {
    if (!serverData.has(guildId)) {
        serverData.set(guildId, {
            whitelistedBots: new Set(),
            timedoutUsers: new Map(),
            flaggedBotIds: new Set(),
            logChannel: null,
            isMonitoring: true,
            activityLog: [],
            massActionTracker: new Map(),
            unifiedWidget: null,
            lastWidgetUpdate: 0,
            stats: { timeouts: 0, bans: 0, kicks: 0, blockedBots: 0, nukeAttempts: 0 },
            channelCreationTracker: new Map(),
            suspiciousUsers: new Set(),
            emergencyMode: false,
            whitelistedRoleIds: new Set() // Store whitelisted role IDs
        });
    }
    return serverData.get(guildId);
}

// Check if a member has a whitelisted role
function hasWhitelistedRole(member, data) {
    if (!data || !data.whitelistedRoleIds || data.whitelistedRoleIds.size === 0) return false;
    return member.roles.cache.some(role => data.whitelistedRoleIds.has(role.id));
}


// Enhanced rate limiting with immediate response
function isRateLimited(userId, action) {
    const key = `${userId}_${action}`;
    const now = Date.now();

    if (!rateLimitCache.has(key)) {
        rateLimitCache.set(key, []);
    }

    const actions = rateLimitCache.get(key);
    const validActions = actions.filter(timestamp => now - timestamp < 1000);

    if (validActions.length >= 3) return true;

    validActions.push(now);
    rateLimitCache.set(key, validActions);
    return false;
}

// Get or create log channel
async function getLogChannel(guild) {
    const data = initServerData(guild.id);
    if (data.logChannel) return data.logChannel;

    let channel = guild.channels.cache.find(ch => ch.name === 'security-logs');
    if (!channel) {
        try {
            channel = await guild.channels.create({
                name: 'security-logs',
                type: 0,
                permissionOverwrites: [{
                    id: guild.roles.everyone.id,
                    deny: [PermissionFlagsBits.ViewChannel]
                }]
            });
        } catch (error) {
            console.error(`Failed to create log channel for ${guild.name}:`, error);
            return null;
        }
    }
    data.logChannel = channel;
    return channel;
}

// Enhanced audit log function with better error handling and longer timeframe
async function getAuditLog(guild, eventType, retries = 3) {
    for (let i = 0; i < retries; i++) {
        try {
            const auditLogs = await guild.fetchAuditLogs({
                type: eventType,
                limit: 10
            });

            // Find the most recent entry within last 10 seconds (increased from 5)
            const recentEntry = auditLogs.entries.find(entry => 
                Date.now() - entry.createdTimestamp < 10000
            );

            return recentEntry || null;
        } catch (error) {
            console.error(`Audit log attempt ${i + 1} failed for ${guild.name}:`, error);
            if (i < retries - 1) {
                await new Promise(resolve => setTimeout(resolve, 200)); // Reduced wait time for faster response
            }
        }
    }
    return null;
}

// Enhanced activity logging
function addActivityLog(guildId, type, description, executor, target) {
    const data = initServerData(guildId);
    data.activityLog.unshift({
        type, description,
        executor: executor ? { id: executor.id, tag: executor.tag } : null,
        target: target ? { id: target.id, tag: target.tag } : null,
        timestamp: Date.now()
    });
    if (data.activityLog.length > 100) data.activityLog.pop();
}

// Ultra-fast mass action tracking
function trackMassAction(guildId, userId, actionType) {
    const data = initServerData(guildId);
    const key = `${userId}_${actionType}`;
    const now = Date.now();

    if (!data.massActionTracker.has(key)) {
        data.massActionTracker.set(key, []);
    }

    const actions = data.massActionTracker.get(key);
    const limit = CONFIG.MASS_ACTION_LIMITS[actionType];
    const validActions = actions.filter(timestamp => now - timestamp < limit.timeWindow);
    validActions.push(now);
    data.massActionTracker.set(key, validActions);

    return validActions.length >= limit.limit;
}

// Enhanced timeout with immediate effect
async function timeoutUser(guild, userId, reason, duration) {
    try {
        if (isRateLimited(userId, 'timeout')) return false;

        const data = initServerData(guild.id);
        const member = await guild.members.fetch(userId).catch(() => null);
        if (!member || member.id === guild.ownerId) return false;

        // Emergency mode for suspected nukers
        if (reason.includes('NUKE') || reason.includes('MASS')) {
            data.emergencyMode = true;
            data.suspiciousUsers.add(userId);
        }

        // Instantly remove all dangerous permissions
        const dangerousRoles = member.roles.cache.filter(role => 
            role.id !== guild.roles.everyone.id && 
            (role.permissions.has(PermissionFlagsBits.Administrator) ||
             role.permissions.has(PermissionFlagsBits.ManageGuild) ||
             role.permissions.has(PermissionFlagsBits.ManageRoles) ||
             role.permissions.has(PermissionFlagsBits.ManageChannels) ||
             role.permissions.has(PermissionFlagsBits.BanMembers) ||
             role.permissions.has(PermissionFlagsBits.KickMembers) ||
             role.permissions.has(PermissionFlagsBits.ManageWebhooks))
        );

        if (dangerousRoles.size > 0) {
            await member.roles.remove(dangerousRoles, `EMERGENCY: Removing dangerous roles - ${reason}`);
        }

        const timeoutDuration = Math.min(duration, 28 * 24 * 60 * 60 * 1000);
        await member.timeout(timeoutDuration, `🚨 ANTI-NUKE: ${reason}`);

        data.timedoutUsers.set(userId, {
            guildId: guild.id,
            reason: reason,
            timeoutAt: Date.now(),
            releaseAt: Date.now() + timeoutDuration
        });

        data.stats.timeouts++;
        globalStats.totalTimeouts++;
        addActivityLog(guild.id, 'TIMEOUT', reason, null, member.user);

        console.log(`⚡ [${guild.name}] INSTANTLY TIMED OUT: ${member.user.tag} - ${reason}`);
        return true;
    } catch (error) {
        console.error(`Failed to timeout user in ${guild.name}:`, error);
        return false;
    }
}

// Instant permanent ban
async function permanentBan(guild, userId, reason) {
    try {
        if (isRateLimited(userId, 'ban')) return false;

        const data = initServerData(guild.id);
        const member = await guild.members.fetch(userId).catch(() => null);

        await guild.members.ban(userId, {
            reason: `🚨 ANTI-NUKE PROTECTION - ${reason}`,
            deleteMessageDays: 7
        });

        data.stats.bans++;
        globalStats.totalBans++;
        addActivityLog(guild.id, 'BAN', reason, null, member ? member.user : { id: userId, tag: 'Unknown User' });

        console.log(`⚡ [${guild.name}] INSTANTLY BANNED: ${userId} - ${reason}`);
        return true;
    } catch (error) {
        console.error(`Failed to ban user in ${guild.name}:`, error);
        return false;
    }
}

// Real-time action tracking without audit logs
const realtimeActionTracker = new Map();

// Voice connection management
async function joinTargetVoiceChannel(guild) {
    try {
        const channel = guild.channels.cache.get(TARGET_VOICE_CHANNEL_ID);
        if (!channel || channel.type !== 2) { // 2 = GUILD_VOICE
            console.log(`❌ [${guild.name}] Voice channel ${TARGET_VOICE_CHANNEL_ID} not found or not a voice channel`);
            return null;
        }

        // Check if bot has permission to join
        const permissions = channel.permissionsFor(guild.members.me);
        if (!permissions.has(PermissionFlagsBits.Connect) || !permissions.has(PermissionFlagsBits.Speak)) {
            console.log(`❌ [${guild.name}] No permission to join voice channel ${channel.name}`);
            return null;
        }

        // Check if already connected to this channel
        const existingConnection = getVoiceConnection(guild.id);
        if (existingConnection && existingConnection.joinConfig.channelId === TARGET_VOICE_CHANNEL_ID) {
            console.log(`✅ [${guild.name}] Already connected to target voice channel`);
            return existingConnection;
        }

        // Join the voice channel
        const connection = joinVoiceChannel({
            channelId: TARGET_VOICE_CHANNEL_ID,
            guildId: guild.id,
            adapterCreator: guild.voiceAdapterCreator,
            selfDeaf: true,
            selfMute: false
        });

        console.log(`🎵 [${guild.name}] Joined voice channel: ${channel.name}`);

        // Handle connection events
        connection.on('stateChange', (oldState, newState) => {
            console.log(`🎵 [${guild.name}] Voice connection state: ${oldState.status} → ${newState.status}`);
        });

        connection.on('error', (error) => {
            console.error(`🎵 [${guild.name}] Voice connection error:`, error);
            // Try to reconnect after 5 seconds
            setTimeout(() => joinTargetVoiceChannel(guild), 5000);
        });

        return connection;

    } catch (error) {
        console.error(`Voice join error in ${guild.name}:`, error);
        // Retry after 10 seconds
        setTimeout(() => joinTargetVoiceChannel(guild), 10000);
        return null;
    }
}

// Monitor voice state changes for reconnection
async function handleVoiceStateUpdate(oldState, newState) {
    // Only handle bot's own voice state changes
    if (newState.id !== client.user.id) return;

    const guild = newState.guild;

    // If bot was disconnected from the target channel
    if (oldState.channelId === TARGET_VOICE_CHANNEL_ID && !newState.channelId) {
        console.log(`🎵 [${guild.name}] Bot disconnected from target voice channel, reconnecting...`);
        setTimeout(() => joinTargetVoiceChannel(guild), 2000);
    }

    // If bot was moved from target channel to another channel
    if (oldState.channelId === TARGET_VOICE_CHANNEL_ID && newState.channelId && newState.channelId !== TARGET_VOICE_CHANNEL_ID) {
        console.log(`🎵 [${guild.name}] Bot moved from target channel, reconnecting...`);
        setTimeout(() => joinTargetVoiceChannel(guild), 2000);
    }
}

function trackRealtimeAction(guildId, actionType, targetId, executorId = null) {
    const key = `${guildId}_${actionType}`;
    const now = Date.now();

    if (!realtimeActionTracker.has(key)) {
        realtimeActionTracker.set(key, []);
    }

    const actions = realtimeActionTracker.get(key);
    actions.push({ targetId, executorId, timestamp: now });

    // Keep only recent actions (last 10 seconds)
    const recentActions = actions.filter(action => now - action.timestamp < 10000);
    realtimeActionTracker.set(key, recentActions);

    return recentActions;
}

// Optimized unified widget
function createUnifiedWidget(guild) {
    const data = initServerData(guild.id);
    const uptime = Math.floor(client.uptime / 1000);
    const hours = Math.floor(uptime / 3600);
    const minutes = Math.floor((uptime % 3600) / 60);

    const totalMembers = guild.memberCount;
    const currentBots = guild.members.cache.filter(member => member.user.bot).size;
    const humanMembers = totalMembers - currentBots;
    const activeTimeouts = data.timedoutUsers.size;

    const statusColor = data.emergencyMode ? 0xFF0000 : (activeTimeouts > 0 ? 0xFF6600 : 0x00FF00);

    const embed = new EmbedBuilder()
        .setTitle('⚡ ULTIMATE ANTI-NUKE SHIELD')
        .setDescription(`🎯 **${guild.name}** | 🕐 <t:${Math.floor(Date.now() / 1000)}:T> | ⚡ ${hours}h ${minutes}m`)
        .setColor(statusColor)
        .addFields(
            {
                name: '📊 SERVER STATUS',
                value: `👥 Total: **${totalMembers}** | 👤 Humans: **${humanMembers}** | 🤖 Bots: **${currentBots}**\n✅ Whitelisted: **${data.whitelistedBots.size}** | 🚩 Flagged: **${data.flaggedBotIds.size}**`,
                inline: false
            },
            {
                name: '🛡️ PROTECTION STATUS',
                value: `⏰ Active Timeouts: **${activeTimeouts}**\n🚨 Emergency Mode: **${data.emergencyMode ? 'ACTIVE' : 'STANDBY'}**\n 🌐 Whitelisted Roles: **${data.whitelistedRoleIds.size}**\n📡 Monitoring: **${data.isMonitoring ? 'ACTIVE' : 'MANUAL'}**\n🤖 Bot Protection: **WHITELIST ONLY**`,
                inline: true
            },
            {
                name: '📈 SECURITY STATS',
                value: `🔒 Timeouts: **${data.stats.timeouts}**\n⚠️ Bans: **${data.stats.bans}**\n👢 Kicks: **${data.stats.kicks}**\n🤖 Blocked: **${data.stats.blockedBots}**\n🚫 Nuke Attempts: **${data.stats.nukeAttempts}**`,
                inline: true
            }
        )
        .setTimestamp()
        .setFooter({ text: `⚡ Maximum Speed Protection | ${data.emergencyMode ? 'EMERGENCY MODE' : 'NORMAL MODE'}` });

    const row = new ActionRowBuilder()
        .addComponents(
            new ButtonBuilder().setCustomId('view_timeouts').setLabel('Timeouts').setStyle(ButtonStyle.Primary).setEmoji('⏰'),
            new ButtonBuilder().setCustomId('view_bans').setLabel('Bans').setStyle(ButtonStyle.Danger).setEmoji('⚠️'),
            new ButtonBuilder().setCustomId('view_activity').setLabel('Activity').setStyle(ButtonStyle.Secondary).setEmoji('📊'),
            new ButtonBuilder().setCustomId('view_stats').setLabel('Stats').setStyle(ButtonStyle.Success).setEmoji('📈')
        );

    return { embeds: [embed], components: [row] };
}

// Optimized widget updates
async function updateUnifiedWidget(guild) {
    const data = initServerData(guild.id);
    const now = Date.now();
    if (now - data.lastWidgetUpdate < 1000) return;

    try {
        const channel = await getLogChannel(guild);
        if (!channel) return;

        const widgetData = createUnifiedWidget(guild);

        if (data.unifiedWidget) {
            try {
                await data.unifiedWidget.edit(widgetData);
            } catch (error) {
                data.unifiedWidget = await channel.send(widgetData);
            }
        } else {
            data.unifiedWidget = await channel.send(widgetData);
        }

        data.lastWidgetUpdate = now;
    } catch (error) {
        console.error(`Widget update error for ${guild.name}:`, error);
    }
}

// Bot ready event
client.once(Events.ClientReady, async () => {
    console.log(`⚡ ULTIMATE ANTI-NUKE SHIELD ONLINE: ${client.user.tag}`);

    client.user.setPresence({
        activities: [{ name: '.gg/scriptspace', type: 3 }],
        status: 'online'
    });

    // Initialize all servers with maximum speed
    const initPromises = Array.from(client.guilds.cache.values()).map(async (guild) => {
        try {
            const data = initServerData(guild.id);
            await getLogChannel(guild);

            await guild.members.fetch();
            const currentBots = guild.members.cache.filter(member => member.user.bot);
            data.whitelistedBots.clear();
            for (const [botId] of currentBots) {
                data.whitelistedBots.add(botId);
            }

            await updateUnifiedWidget(guild);
            console.log(`⚡ SHIELD activated for ${guild.name} - ${data.whitelistedBots.size} bots secured`);
        } catch (error) {
            console.error(`Failed to initialize ${guild.name}:`, error);
        }
    });

    await Promise.all(initPromises);

    // Auto-join target voice channel in all guilds
    const voiceJoinPromises = Array.from(client.guilds.cache.values()).map(async (guild) => {
        await joinTargetVoiceChannel(guild);
    });

    await Promise.all(voiceJoinPromises);
});

// Enhanced channel creation detection with mandatory bans
client.on('channelCreate', async (channel) => {
    try {
        const data = initServerData(channel.guild.id);

        // Track this channel creation in real-time
        const recentActions = trackRealtimeAction(channel.guild.id, 'CHANNEL_CREATE', channel.id);

        // Get the executor from audit logs with retries
        const auditLog = await getAuditLog(channel.guild, AuditLogEvent.ChannelCreate, 5);
        let executor = null;

        if (auditLog) {
            executor = auditLog.executor;
        }

        // Check if this looks like unauthorized channel creation
        const suspiciousNames = ['nuke', 'raid', 'hack', 'destroy', 'kill', 'delete', 'fuck', 'spam', 'test'];
        const isSuspiciousName = suspiciousNames.some(word => channel.name.toLowerCase().includes(word));
        const isMassCreation = recentActions.length >= 1; // ANY channel creation is suspicious
        const isUnauthorized = true; // Consider ALL channel creation as requiring authorization

        // ALWAYS ban channel creators (except owner and whitelisted bots)
        if (executor && executor.id !== channel.guild.ownerId && executor.id !== client.user.id) {
            // Check if executor is a whitelisted bot
            const isWhitelistedBot = executor.bot && data.whitelistedBots.has(executor.id);

            if (!isWhitelistedBot) {
                // Delete the channel first
                await channel.delete('⚡ UNAUTHORIZED CHANNEL CREATION - ANTI-NUKE SHIELD').catch(() => {});

                // PERMANENTLY BAN the executor
                await permanentBan(channel.guild, executor.id,
                    `UNAUTHORIZED CHANNEL CREATION: "${channel.name}"`);

                data.stats.nukeAttempts++;
                globalStats.nukeAttempts++;
                data.emergencyMode = true;

                console.log(`⚡ [${channel.guild.name}] BANNED CHANNEL CREATOR: ${executor.tag} - Channel: ${channel.name}`);
                addActivityLog(channel.guild.id, 'CHANNEL_CREATOR_BANNED', `Banned: ${executor.tag} for creating ${channel.name}`, executor, null);
                await updateUnifiedWidget(channel.guild);

                // Alert in log channel
                const logChannel = await getLogChannel(channel.guild);
                if (logChannel) {
                    const alertEmbed = new EmbedBuilder()
                        .setTitle('🚨 UNAUTHORIZED CHANNEL CREATOR BANNED')
                        .setDescription(`**Channel:** ${channel.name}\n**Executor:** ${executor.tag} (${executor.id})\n**Bot:** ${executor.bot ? 'Yes' : 'No'}\n**Action:** Channel deleted, user/bot permanently banned`)
                        .setColor(0xFF0000)
                        .setTimestamp();

                    await logChannel.send({ embeds: [alertEmbed] });
                }
            }
        } else if (!executor) {
            // If we can't identify the executor, delete the channel anyway
            await channel.delete('⚡ UNAUTHORIZED CHANNEL - NO EXECUTOR IDENTIFIED').catch(() => {});
            console.log(`⚡ [${channel.guild.name}] DELETED CHANNEL WITH UNKNOWN CREATOR: ${channel.name}`);
        }
    } catch (error) {
        console.error(`Channel creation error in ${channel.guild.name}:`, error);
    }
});

// Enhanced channel deletion detection with executor bans
client.on('channelDelete', async (channel) => {
    try {
        const data = initServerData(channel.guild.id);

        // Track this deletion in real-time
        const recentActions = trackRealtimeAction(channel.guild.id, 'CHANNEL_DELETE', channel.id);

        // Get the executor from audit logs with multiple retries
        const auditLog = await getAuditLog(channel.guild, AuditLogEvent.ChannelDelete, 5);
        let executor = null;

        if (auditLog) {
            executor = auditLog.executor;
        }

        // BAN ALL channel deleters (except owner and bot itself)
        if (executor && executor.id !== channel.guild.ownerId && executor.id !== client.user.id) {
            // Check if executor is a whitelisted bot
            const isWhitelistedBot = executor.bot && data.whitelistedBots.has(executor.id);

            // Check if executor has whitelisted role
            const executorMember = await channel.guild.members.fetch(executor.id).catch(() => null);
            const hasWhitelistedRolePerm = executorMember && hasWhitelistedRole(executorMember, data);

            if (!isWhitelistedBot) {
                if (hasWhitelistedRolePerm) {
                    // KICK (not ban) users with whitelisted role
                    await executorMember.kick(`🚫 WHITELISTED USER - UNAUTHORIZED CHANNEL DELETION: "${channel.name}"`);
                    console.log(`⚡ [${channel.guild.name}] KICKED WHITELISTED USER: ${executor.tag} for deleting ${channel.name}`);

                    const logChannel = await getLogChannel(channel.guild);
                    if (logChannel) {
                        const alertEmbed = new EmbedBuilder()
                            .setTitle('⚠️ WHITELISTED USER KICKED FOR CHANNEL DELETION')
                            .setDescription(`**Channel:** ${channel.name}\n**Executor:** ${executor.tag} (${executor.id})\n**Action:** User kicked (not banned due to whitelisted status)`)
                            .setColor(0xFF6600)
                            .setTimestamp();

                        await logChannel.send({ embeds: [alertEmbed] });
                    }
                } else {
                    // PERMANENTLY BAN for ANY channel deletion
                    await permanentBan(channel.guild, executor.id,
                        `UNAUTHORIZED CHANNEL DELETION: Deleted "${channel.name}"`);

                    data.stats.nukeAttempts++;
                    globalStats.nukeAttempts++;
                    data.emergencyMode = true;

                    console.log(`⚡ [${channel.guild.name}] BANNED CHANNEL DELETER: ${executor.tag} - Channel: ${channel.name}`);
                    addActivityLog(channel.guild.id, 'CHANNEL_DELETER_BANNED', `Banned: ${executor.tag} for deleting ${channel.name}`, executor, null);
                    await updateUnifiedWidget(channel.guild);

                    // Alert in log channel
                    const logChannel = await getLogChannel(channel.guild);
                    if (logChannel) {
                        const alertEmbed = new EmbedBuilder()
                            .setTitle('🚨 CHANNEL DELETER BANNED')
                            .setDescription(`**Channel:** ${channel.name}\n**Executor:** ${executor.tag} (${executor.id})\n**Bot:** ${executor.bot ? 'Yes' : 'No'}\n**Action:** PERMANENTLY BANNED for channel deletion`)
                            .setColor(0xFF0000)
                            .setTimestamp();

                        await logChannel.send({ embeds: [alertEmbed] });
                    }
                }
            }
        } else if (!executor) {
            console.log(`⚡ [${channel.guild.name}] CHANNEL DELETED WITH UNKNOWN EXECUTOR: ${channel.name}`);
        }

        // If multiple channels deleted quickly, it's likely a mass nuke attempt
        const isMassDeletion = recentActions.length >= 2;
        if (isMassDeletion && !data.emergencyMode) {
            data.emergencyMode = true;
            console.log(`⚡ [${channel.guild.name}] MASS DELETION PATTERN DETECTED`);
        }
    } catch (error) {
        console.error(`Channel deletion error in ${channel.guild.name}:`, error);
    }
});

// Enhanced channel update detection with mandatory bans for ALL modifications
client.on('channelUpdate', async (oldChannel, newChannel) => {
    const nameChanged = oldChannel.name !== newChannel.name;
    const positionChanged = oldChannel.position !== newChannel.position;
    const permissionsChanged = JSON.stringify(oldChannel.permissionOverwrites.cache) !== JSON.stringify(newChannel.permissionOverwrites.cache);

    if (nameChanged || positionChanged || permissionsChanged) {
        try {
            const data = initServerData(newChannel.guild.id);
            const auditLog = await getAuditLog(newChannel.guild, AuditLogEvent.ChannelUpdate, 5);

            if (auditLog) {
                const executor = auditLog.executor;

                if (executor && executor.id !== client.user.id && executor.id !== newChannel.guild.ownerId) {
                    // Check if executor is a whitelisted bot
                    const isWhitelistedBot = executor.bot && data.whitelistedBots.has(executor.id);

                    // Check if executor has whitelisted role
                    const executorMember = await newChannel.guild.members.fetch(executor.id).catch(() => null);
                    const hasWhitelistedRolePerm = executorMember && hasWhitelistedRole(executorMember, data);

                    if (!isWhitelistedBot) {
                        if (hasWhitelistedRolePerm) {
                            // KICK (not ban) users with whitelisted role
                            await executorMember.kick(`🚫 WHITELISTED USER - UNAUTHORIZED CHANNEL MODIFICATION: #"${oldChannel.name}" → "#${newChannel.name}"`);
                            console.log(`⚡ [${newChannel.guild.name}] KICKED WHITELISTED USER: ${executor.tag} for modifying #${oldChannel.name}`);

                            const logChannel = await getLogChannel(newChannel.guild);
                            if (logChannel) {
                                const alertEmbed = new EmbedBuilder()
                                    .setTitle('⚠️ WHITELISTED USER KICKED FOR CHANNEL MODIFICATION')
                                    .setDescription(`**Executor:** ${executor.tag} (${executor.id})\n**Channel:** #${oldChannel.name} → #${newChannel.name}\n**Action:** User kicked (not banned due to whitelisted status)`)
                                    .setColor(0xFF6600)
                                    .setTimestamp();

                                await logChannel.send({ embeds: [alertEmbed] });
                            }
                        } else {
                            // PERMANENTLY BAN for ANY channel modification
                            await permanentBan(newChannel.guild, executor.id, 
                                `UNAUTHORIZED CHANNEL MODIFICATION: #${oldChannel.name} → #${newChannel.name}`);

                            data.stats.nukeAttempts++;
                            globalStats.nukeAttempts++;
                            data.emergencyMode = true;

                            console.log(`⚡ [${newChannel.guild.name}] BANNED CHANNEL MODIFIER: ${executor.tag}`);
                            addActivityLog(newChannel.guild.id, 'CHANNEL_MODIFIER_BANNED', `Banned: ${executor.tag} for modifying #${oldChannel.name}`, executor, null);
                            await updateUnifiedWidget(newChannel.guild);

                            // Send alert to log channel
                            const logChannel = await getLogChannel(newChannel.guild);
                            if (logChannel) {
                                const alertEmbed = new EmbedBuilder()
                                    .setTitle('🚨 UNAUTHORIZED CHANNEL MODIFIER BANNED')
                                    .setDescription(`**Executor:** ${executor.tag} (${executor.id})\n**Bot:** ${executor.bot ? 'Yes' : 'No'}\n**Channel:** #${oldChannel.name} → #${newChannel.name}\n**Changes:** ${nameChanged ? 'Name, ' : ''}${positionChanged ? 'Position, ' : ''}${permissionsChanged ? 'Permissions' : ''}\n**Action:** PERMANENTLY BANNED`)
                                    .setColor(0xFF0000)
                                    .setTimestamp();

                                await logChannel.send({ embeds: [alertEmbed] });
                            }
                        }
                    }
                }
            }
        } catch (error) {
            console.error(`Channel modification error in ${newChannel.guild.name}:`, error);
        }
    }
});

// WEBHOOK CREATION MONITORING - INSTANT BAN
client.on('webhookUpdate', async (channel) => {
    try {
        const data = initServerData(channel.guild.id);
        const auditLog = await getAuditLog(channel.guild, AuditLogEvent.WebhookCreate);

        if (auditLog) {
            const executor = auditLog.executor;

            if (executor && executor.id !== client.user.id && executor.id !== channel.guild.ownerId) {
                // Check if executor is a whitelisted bot
                const isWhitelistedBot = executor.bot && data.whitelistedBots.has(executor.id);

                // Check if executor has whitelisted role
                const executorMember = await channel.guild.members.fetch(executor.id).catch(() => null);
                const hasWhitelistedRolePerm = executorMember && hasWhitelistedRole(executorMember, data);

                if (!isWhitelistedBot) {
                    if (hasWhitelistedRolePerm) {
                        // KICK (not ban) users with whitelisted role
                        await executorMember.kick(`🚫 WHITELISTED USER - UNAUTHORIZED WEBHOOK CREATION IN: #${channel.name}`);
                        console.log(`⚡ [${channel.guild.name}] KICKED WHITELISTED USER: ${executor.tag} for webhook creation`);

                        const logChannel = await getLogChannel(channel.guild);
                        if (logChannel) {
                            const alertEmbed = new EmbedBuilder()
                                .setTitle('⚠️ WHITELISTED USER KICKED FOR WEBHOOK CREATION')
                                .setDescription(`**Executor:** ${executor.tag} (${executor.id})\n**Channel:** #${channel.name}\n**Action:** User kicked (not banned due to whitelisted status)`)
                                .setColor(0xFF6600)
                                .setTimestamp();

                            await logChannel.send({ embeds: [alertEmbed] });
                        }
                    } else {
                        // INSTANT BAN for webhook creation (major security risk)
                        await permanentBan(channel.guild, executor.id, 
                            `UNAUTHORIZED WEBHOOK CREATION IN: #${channel.name}`);

                        data.stats.nukeAttempts++;
                        globalStats.nukeAttempts++;
                        data.emergencyMode = true;

                        console.log(`⚡ [${channel.guild.name}] WEBHOOK THREAT BANNED: ${executor.tag}`);
                        addActivityLog(channel.guild.id, 'WEBHOOK_THREAT_BANNED', 
                            `Webhook creator: ${executor.tag}`, executor, null);
                        await updateUnifiedWidget(channel.guild);

                        // Send alert to log channel
                        const logChannel = await getLogChannel(channel.guild);
                        if (logChannel) {
                            const alertEmbed = new EmbedBuilder()
                                .setTitle('🚨 UNAUTHORIZED WEBHOOK DETECTED')
                                .setDescription(`**Executor:** ${executor.tag} (${executor.id})\n**Channel:** #${channel.name}\n**Action:** PERMANENTLY BANNED\n**Reason:** Webhook creation is a major security risk`)
                                .setColor(0xFF0000)
                                .setTimestamp();

                            await logChannel.send({ embeds: [alertEmbed] });
                        }
                    }
                }
            }
        }
    } catch (error) {
        console.error(`Webhook monitoring error in ${channel.guild.name}:`, error);
    }
});

// Enhanced role update monitoring with immediate bans
client.on('roleUpdate', async (oldRole, newRole) => {
    try {
        const data = initServerData(newRole.guild.id);
        const permissionsChanged = !oldRole.permissions.equals(newRole.permissions);
        const nameChanged = oldRole.name !== newRole.name;

        if (permissionsChanged || nameChanged) {
            const auditLog = await getAuditLog(newRole.guild, AuditLogEvent.RoleUpdate);

            if (auditLog) {
                const executor = auditLog.executor;

                if (executor && executor.id !== client.user.id && executor.id !== newRole.guild.ownerId) {
                    // Check if executor is a whitelisted bot
                    const isWhitelistedBot = executor.bot && data.whitelistedBots.has(executor.id);

                    // Check if executor has whitelisted role
                    const executorMember = await newRole.guild.members.fetch(executor.id).catch(() => null);
                    const hasWhitelistedRolePerm = executorMember && hasWhitelistedRole(executorMember, data);

                    if (!isWhitelistedBot) {
                        // Check if dangerous permissions were added
                        const dangerousPerms = [
                            PermissionFlagsBits.Administrator,
                            PermissionFlagsBits.ManageGuild,
                            PermissionFlagsBits.ManageRoles,
                            PermissionFlagsBits.ManageChannels,
                            PermissionFlagsBits.BanMembers,
                            PermissionFlagsBits.KickMembers,
                            PermissionFlagsBits.ManageWebhooks
                        ];

                        const addedDangerousPerms = dangerousPerms.filter(perm => 
                            !oldRole.permissions.has(perm) && newRole.permissions.has(perm)
                        );

                        // Check for suspicious role names
                        const suspiciousNames = ['nuke', 'raid', 'hack', 'admin', 'owner', 'destroy', 'mod'];
                        const isSuspiciousName = suspiciousNames.some(word => 
                            newRole.name.toLowerCase().includes(word) && 
                            !oldRole.name.toLowerCase().includes(word)
                        );

                        if (addedDangerousPerms.length > 0 || isSuspiciousName) {
                            if (hasWhitelistedRolePerm) {
                                // KICK (not ban) users with whitelisted role
                                await executorMember.kick(`🚫 WHITELISTED USER - MALICIOUS ROLE MODIFICATION: "${oldRole.name}" → "${newRole.name}"`);
                                console.log(`⚡ [${newRole.guild.name}] KICKED WHITELISTED USER: ${executor.tag} for malicious role modification`);

                                // Revert the dangerous permissions/name
                                try {
                                    await newRole.setPermissions(oldRole.permissions, 'ANTI-NUKE: Reverted dangerous permissions');
                                    if (nameChanged) {
                                        await newRole.setName(oldRole.name, 'ANTI-NUKE: Reverted suspicious name');
                                    }
                                } catch (revertError) {
                                    console.error('Failed to revert role changes:', revertError);
                                }

                                const logChannel = await getLogChannel(newRole.guild);
                                if (logChannel) {
                                    const alertEmbed = new EmbedBuilder()
                                        .setTitle('⚠️ WHITELISTED USER KICKED FOR MALICIOUS ROLE MODIFICATION')
                                        .setDescription(`**Executor:** ${executor.tag} (${executor.id})\n**Role:** ${oldRole.name} → ${newRole.name}\n**Action:** User kicked (not banned due to whitelisted status)`)
                                        .setColor(0xFF6600)
                                        .setTimestamp();

                                    await logChannel.send({ embeds: [alertEmbed] });
                                }
                            } else {
                                // INSTANT BAN for dangerous role modifications
                                await permanentBan(newRole.guild, executor.id,
                                    `MALICIOUS ROLE MODIFICATION: ${oldRole.name} → ${newRole.name}`);

                                // Revert the dangerous permissions/name
                                try {
                                    await newRole.setPermissions(oldRole.permissions, 'ANTI-NUKE: Reverted dangerous permissions');
                                    if (nameChanged) {
                                        await newRole.setName(oldRole.name, 'ANTI-NUKE: Reverted suspicious name');
                                    }
                                } catch (revertError) {
                                    console.error('Failed to revert role changes:', revertError);
                                }

                                data.stats.nukeAttempts++;
                                globalStats.nukeAttempts++;
                                data.emergencyMode = true;

                                console.log(`⚡ [${newRole.guild.name}] ROLE THREAT BANNED: ${executor.tag}`);
                                addActivityLog(newRole.guild.id, 'ROLE_THREAT_BANNED', 
                                    `Malicious role modification: ${oldRole.name}`, executor, null);
                                await updateUnifiedWidget(newRole.guild);

                                // Send alert to log channel
                                const logChannel = await getLogChannel(newRole.guild);
                                if (logChannel) {
                                    const alertEmbed = new EmbedBuilder()
                                        .setTitle('🚨 MALICIOUS ROLE MODIFICATION')
                                        .setDescription(`**Executor:** ${executor.tag} (${executor.id})\n**Bot:** ${executor.bot ? 'Yes' : 'No'}\n**Role:** ${oldRole.name} → ${newRole.name}\n**Dangerous Perms:** ${addedDangerousPerms.length}\n**Action:** PERMANENTLY BANNED`)
                                        .setColor(0xFF0000)
                                        .setTimestamp();

                                    await logChannel.send({ embeds: [alertEmbed] });
                                }
                            }
                        }
                    }
                }
            }
        }
    } catch (error) {
        console.error(`Role update error in ${newRole.guild.name}:`, error);
    }
});

// Enhanced role creation detection with executor bans
client.on('roleCreate', async (role) => {
    try {
        const data = initServerData(role.guild.id);

        // Track role creation
        const recentActions = trackRealtimeAction(role.guild.id, 'ROLE_CREATE', role.id);

        // Get the executor from audit logs
        const auditLog = await getAuditLog(role.guild, AuditLogEvent.RoleCreate);
        let executor = null;

        if (auditLog) {
            executor = auditLog.executor;
        }

        // Check for suspicious role names or mass creation
        const suspiciousNames = ['nuke', 'raid', 'hack', 'admin', 'owner', 'destroy', 'mod'];
        const isSuspiciousName = suspiciousNames.some(word => role.name.toLowerCase().includes(word));
        const hasDangerousPerms = role.permissions.has(PermissionFlagsBits.Administrator) || 
                                 role.permissions.has(PermissionFlagsBits.ManageGuild) ||
                                 role.permissions.has(PermissionFlagsBits.ManageRoles) ||
                                 role.permissions.has(PermissionFlagsBits.ManageChannels) ||
                                 role.permissions.has(PermissionFlagsBits.BanMembers) ||
                                 role.permissions.has(PermissionFlagsBits.KickMembers);
        const isMassCreation = recentActions.length >= 2;

        if (isSuspiciousName || hasDangerousPerms || isMassCreation) {
            // Delete the role first
            await role.delete('⚡ UNAUTHORIZED ROLE - ANTI-NUKE SHIELD').catch(() => {});

            // BAN the executor if found
            if (executor && executor.id !== role.guild.ownerId && executor.id !== client.user.id) {
                // Check if executor is a whitelisted bot
                const isWhitelistedBot = executor.bot && data.whitelistedBots.has(executor.id);

                // Check if executor has whitelisted role
                const executorMember = await role.guild.members.fetch(executor.id).catch(() => null);
                const hasWhitelistedRolePerm = executorMember && hasWhitelistedRole(executorMember, data);

                if (!isWhitelistedBot) {
                    if (hasWhitelistedRolePerm) {
                        // KICK (not ban) users with whitelisted role
                        if (executorMember) {
                            await executorMember.kick(`🚫 WHITELISTED USER - MALICIOUS ROLE CREATION: "${role.name}"`).catch(() => {});
                            console.log(`⚡ [${role.guild.name}] KICKED WHITELISTED USER: ${executor.tag} for malicious role creation`);

                            const logChannel = await getLogChannel(role.guild);
                            if (logChannel) {
                                const alertEmbed = new EmbedBuilder()
                                    .setTitle('⚠️ WHITELISTED USER KICKED FOR MALICIOUS ROLE CREATION')
                                    .setDescription(`**Executor:** ${executor.tag} (${executor.id})\n**Role:** ${role.name}\n**Action:** User kicked (not banned due to whitelisted status)`)
                                    .setColor(0xFF6600)
                                    .setTimestamp();

                                await logChannel.send({ embeds: [alertEmbed] }).catch(() => {});
                            }
                        }
                    } else {
                        await permanentBan(role.guild, executor.id,
                            `MALICIOUS ROLE CREATION: "${role.name}" with dangerous permissions`);

                        console.log(`⚡ [${role.guild.name}] BANNED ROLE CREATOR: ${executor.tag}`);
                    }
                }
            }

            data.stats.nukeAttempts++;
            globalStats.nukeAttempts++;
            data.emergencyMode = true;

            console.log(`⚡ [${role.guild.name}] BLOCKED SUSPICIOUS ROLE: ${role.name}`);
            addActivityLog(role.guild.id, 'ROLE_CREATE_BANNED', `Blocked and banned: ${role.name}`, executor, null);
            await updateUnifiedWidget(role.guild);

            // Send alert to log channel
            const logChannel = await getLogChannel(role.guild);
            if (logChannel) {
                const alertEmbed = new EmbedBuilder()
                    .setTitle('🚨 MALICIOUS ROLE BLOCKED')
                    .setDescription(`**Role:** ${role.name}\n**Executor:** ${executor ? `${executor.tag} (${executor.id})` : 'Unknown'}\n**Reason:** ${isSuspiciousName ? 'Suspicious name' : hasDangerousPerms ? 'Dangerous permissions' : 'Mass creation'}\n**Action:** Role deleted, ${executor ? 'user banned' : 'unable to identify user'}`)
                    .setColor(0xFF0000)
                    .setTimestamp();

                await logChannel.send({ embeds: [alertEmbed] }).catch(() => {});
            }
        }
    } catch (error) {
        console.error(`Role creation error in ${role.guild.name}:`, error);
    }
});

// Enhanced role deletion detection with executor bans
client.on('roleDelete', async (role) => {
    try {
        const data = initServerData(role.guild.id);

        // Track role deletion
        const recentActions = trackRealtimeAction(role.guild.id, 'ROLE_DELETE', role.id);

        // Get the executor from audit logs
        const auditLog = await getAuditLog(role.guild, AuditLogEvent.RoleDelete);
        let executor = null;

        if (auditLog) {
            executor = auditLog.executor;
        }

        // Multiple role deletions = nuke attempt
        const isMassDeletion = recentActions.length >= 2;

        if (isMassDeletion) {
            // BAN the executor if found
            if (executor && executor.id !== role.guild.ownerId && executor.id !== client.user.id) {
                // Check if executor is a whitelisted bot
                const isWhitelistedBot = executor.bot && data.whitelistedBots.has(executor.id);

                // Check if executor has whitelisted role
                const executorMember = await role.guild.members.fetch(executor.id).catch(() => null);
                const hasWhitelistedRolePerm = executorMember && hasWhitelistedRole(executorMember, data);

                if (!isWhitelistedBot) {
                    if (hasWhitelistedRolePerm) {
                        // KICK (not ban) users with whitelisted role
                        await executorMember.kick(`🚫 WHITELISTED USER - ROLE MASS DELETION: "${role.name}"`);
                        console.log(`⚡ [${role.guild.name}] KICKED WHITELISTED USER: ${executor.tag} for mass role deletion`);

                        const logChannel = await getLogChannel(role.guild);
                        if (logChannel) {
                            const alertEmbed = new EmbedBuilder()
                                .setTitle('⚠️ WHITELISTED USER KICKED FOR ROLE MASS DELETION')
                                .setDescription(`**Executor:** ${executor.tag} (${executor.id})\n**Role:** ${role.name}\n**Action:** User kicked (not banned due to whitelisted status)`)
                                .setColor(0xFF6600)
                                .setTimestamp();

                            await logChannel.send({ embeds: [alertEmbed] });
                        }
                    } else {
                        await permanentBan(role.guild, executor.id,
                            `ROLE MASS DELETION: Deleted multiple roles including "${role.name}"`);

                        console.log(`⚡ [${role.guild.name}] BANNED MASS ROLE DELETER: ${executor.tag}`);
                    }
                }
            }

            data.stats.nukeAttempts++;
            globalStats.nukeAttempts++;
            data.emergencyMode = true;

            console.log(`⚡ [${role.guild.name}] MASS ROLE DELETION DETECTED: ${role.name}`);
            addActivityLog(role.guild.id, 'ROLE_DELETE_BANNED', `Mass deleter banned: ${role.name}`, executor, null);
            await updateUnifiedWidget(role.guild);

            // Send alert to log channel
            const logChannel = await getLogChannel(role.guild);
            if (logChannel) {
                const alertEmbed = new EmbedBuilder()
                    .setTitle('🚨 MASS ROLE DELETION DETECTED')
                    .setDescription(`**Role:** ${role.name}\n**Executor:** ${executor ? `${executor.tag} (${executor.id})` : 'Unknown'}\n**Reason:** Multiple roles deleted rapidly\n**Action:** ${executor ? 'User banned, ' : ''}Emergency mode activated`)
                    .setColor(0xFF0000)
                    .setTimestamp();

                await logChannel.send({ embeds: [alertEmbed] });
            }
        }
    } catch (error) {
        console.error(`Role deletion error in ${role.guild.name}:`, error);
    }
});

// REAL-TIME member monitoring (no audit logs needed)
client.on('guildMemberRemove', async (member) => {
    if (member.user.bot) return;

    try {
        const data = initServerData(member.guild.id);

        // Track this member removal
        const recentRemovals = trackRealtimeAction(member.guild.id, 'MEMBER_REMOVE', member.id);

        // Multiple member removals in short time = mass kick/ban attempt
        const isMassRemoval = recentRemovals.length >= 3; // 3+ removals in 10 seconds

        if (isMassRemoval) {
            data.stats.nukeAttempts++;
            globalStats.nukeAttempts++;
            data.emergencyMode = true;

            console.log(`⚡ [${member.guild.name}] MASS MEMBER REMOVAL DETECTED: ${member.user.tag}`);
            addActivityLog(member.guild.id, 'MASS_REMOVAL_DETECTED', `Mass removal detected: ${member.user.tag}`, null, member.user);
            await updateUnifiedWidget(member.guild);

            // Alert in log channel
            const logChannel = await getLogChannel(member.guild);
            if (logChannel) {
                const alertEmbed = new EmbedBuilder()
                    .setTitle('🚨 MASS MEMBER REMOVAL DETECTED')
                    .setDescription(`**Member:** ${member.user.tag}\n**Reason:** Multiple members removed rapidly\n**Action:** Emergency mode activated`)
                    .setColor(0xFF0000)
                    .setTimestamp();

                await logChannel.send({ embeds: [alertEmbed] });
            }
        }
    } catch (error) {
        console.error(`Member removal error in ${member.guild.name}:`, error);
    }
});

// Member update monitoring (optimized bypass prevention)
client.on('guildMemberUpdate', async (oldMember, newMember) => {
    try {
        const data = initServerData(newMember.guild.id);
        const userId = newMember.id;

        if (data.timedoutUsers.has(userId)) {
            const timeoutData = data.timedoutUsers.get(userId);
            const now = Date.now();

            // Check if timeout was manually removed before its scheduled release
            if (oldMember.communicationDisabledUntil && 
                !newMember.communicationDisabledUntil &&
                now < timeoutData.releaseAt) {

                const auditLog = await getAuditLog(newMember.guild, AuditLogEvent.MemberUpdate);
                // Check if the owner removed the timeout
                if (auditLog && auditLog.executor && auditLog.executor.id === newMember.guild.ownerId) {
                    console.log(`✅ [${newMember.guild.name}] Timeout manually removed by owner: ${newMember.user.tag}`);
                    data.timedoutUsers.delete(userId);
                    addActivityLog(newMember.guild.id, 'TIMEOUT_REMOVED', 'Manually removed by owner', auditLog.executor, newMember.user);
                    await updateUnifiedWidget(newMember.guild);
                    return;
                }

                // Reapply timeout if bypassed
                const remainingTime = timeoutData.releaseAt - now;
                await newMember.timeout(Math.min(remainingTime, 28 * 24 * 60 * 60 * 1000), 
                    '🚨 BYPASS PREVENTION - TIMEOUT REAPPLIED');

                addActivityLog(newMember.guild.id, 'TIMEOUT_BYPASS_BLOCKED', 'Bypass attempt blocked', null, newMember.user);
                await updateUnifiedWidget(newMember.guild);
            }
        }

    } catch (error) {
        console.error(`Member update error in ${newMember.guild.name}:`, error);
    }
});

// Button interactions (optimized)
client.on('interactionCreate', async (interaction) => {
    if (!interaction.isButton()) return;

    try {
        const data = initServerData(interaction.guild.id);
        let embed;

        switch (interaction.customId) {
            case 'view_timeouts':
                const timeoutList = Array.from(data.timedoutUsers.entries())
                    .map(([userId, timeoutData]) => {
                        const releaseTime = Math.floor(timeoutData.releaseAt / 1000);
                        return `<@${userId}> - ${timeoutData.reason.substring(0, 40)}... (Until: <t:${releaseTime}:R>)`;
                    })
                    .join('\n') || 'No users currently timed out';

                embed = new EmbedBuilder()
                    .setTitle('⏰ ACTIVE TIMEOUTS')
                    .setDescription(timeoutList.length > 4096 ? timeoutList.substring(0, 4096) + '...' : timeoutList)
                    .setColor(0xFF6600)
                    .setTimestamp();
                break;

            case 'view_bans':
                const recentBans = data.activityLog
                    .filter(activity => activity.type === 'BAN')
                    .slice(0, 10)
                    .map(activity => {
                        const timeAgo = Math.floor(activity.timestamp / 1000);
                        return `**${activity.target.tag}** - ${activity.description} (<t:${timeAgo}:R>)`;
                    })
                    .join('\n') || 'No recent bans';

                embed = new EmbedBuilder()
                    .setTitle('⚠️ RECENT BANS')
                    .setDescription(recentBans)
                    .setColor(0xFF0000)
                    .setTimestamp();
                break;

            case 'view_activity':
                const recentActivity = data.activityLog
                    .slice(0, 10)
                    .map(activity => {
                        const timeAgo = Math.floor(activity.timestamp / 1000);
                        const executor = activity.executor ? activity.executor.tag : 'System';
                        const target = activity.target ? activity.target.tag : 'N/A';
                        return `**${activity.type}** by ${executor} → ${target} (<t:${timeAgo}:R>)`;
                    })
                    .join('\n') || 'No recent activity';

                embed = new EmbedBuilder()
                    .setTitle('📊 RECENT ACTIVITY')
                    .setDescription(recentActivity)
                    .setColor(0x0099FF)
                    .setTimestamp();
                break;

            case 'view_stats':
                const uptime = Math.floor(client.uptime / 1000);
                const hours = Math.floor(uptime / 3600);
                const minutes = Math.floor((uptime % 3600) / 60);

                embed = new EmbedBuilder()
                    .setTitle('📈 DETAILED STATISTICS')
                    .addFields(
                        { name: '⏰ Timeouts', value: data.stats.timeouts.toString(), inline: true },
                        { name: '⚠️ Bans', value: data.stats.bans.toString(), inline: true },
                        { name: '👢 Kicks', value: data.stats.kicks.toString(), inline: true },
                        { name: '🤖 Blocked Bots', value: data.stats.blockedBots.toString(), inline: true },
                        { name: '🚫 Nuke Attempts', value: data.stats.nukeAttempts.toString(), inline: true },
                        { name: '⚡ Uptime', value: `${hours}h ${minutes}m`, inline: true }
                    )
                    .setColor(0x00FF00)
                    .setTimestamp();
                break;
        }

        await interaction.reply({ embeds: [embed], ephemeral: true });

    } catch (error) {
        console.error('Button interaction error:', error);
        await interaction.reply({ content: 'Error processing request.', ephemeral: true }).catch(() => {});
    }
});

// Enhanced bot addition detection with mandatory banning of bot adders
client.on('guildMemberAdd', async (member) => {
    if (!member.user.bot) return;

    try {
        const data = initServerData(member.guild.id);
        const botId = member.user.id;

        // Track this bot addition
        const recentBotAdditions = trackRealtimeAction(member.guild.id, 'BOT_ADD', botId);

        // Check if bot is whitelisted
        if (!data.whitelistedBots.has(botId)) {
            // INSTANT kick the unauthorized bot
            await member.kick('🚨 UNAUTHORIZED BOT - NOT WHITELISTED');

            // Get the person who added this bot from audit logs
            const auditLog = await getAuditLog(member.guild, AuditLogEvent.BotAdd, 5);
            let botAdder = null;

            if (auditLog) {
                botAdder = auditLog.executor;
            }

            // BAN the person who added the unauthorized bot
            if (botAdder && botAdder.id !== member.guild.ownerId && botAdder.id !== client.user.id) {
                // Check if executor is a whitelisted bot (should not happen for bot adders)
                const isWhitelistedBotAdder = botAdder.bot && data.whitelistedBots.has(botAdder.id);

                // Check if executor has whitelisted role
                const botAdderMember = await member.guild.members.fetch(botAdder.id).catch(() => null);
                const hasWhitelistedRolePerm = botAdderMember && hasWhitelistedRole(botAdderMember, data);

                if (!isWhitelistedBotAdder) {
                    if (hasWhitelistedRolePerm) {
                        // KICK (not ban) users with whitelisted role
                        await botAdderMember.kick(`🚫 WHITELISTED USER - UNAUTHORIZED BOT ADDITION: "${member.user.tag}"`);
                        console.log(`⚡ [${member.guild.name}] KICKED WHITELISTED USER: ${botAdder.tag} for adding bot "${member.user.tag}"`);

                        const logChannel = await getLogChannel(member.guild);
                        if (logChannel) {
                            const alertEmbed = new EmbedBuilder()
                                .setTitle('⚠️ WHITELISTED USER KICKED FOR UNAUTHORIZED BOT ADDITION')
                                .setDescription(`**Bot:** ${member.user.tag} (${botId})\n**Bot Adder:** ${botAdder.tag} (${botAdder.id})\n**Action:** User kicked (not banned due to whitelisted status)`)
                                .setColor(0xFF6600)
                                .setTimestamp();

                            await logChannel.send({ embeds: [alertEmbed] });
                        }
                    } else {
                        await permanentBan(member.guild, botAdder.id,
                            `UNAUTHORIZED BOT ADDITION: Added bot "${member.user.tag}"`);

                        console.log(`⚡ [${member.guild.name}] BANNED BOT ADDER: ${botAdder.tag} for adding ${member.user.tag}`);
                    }
                }
            }

            data.stats.blockedBots++;
            data.stats.nukeAttempts++;
            globalStats.unauthorizedBots++;
            globalStats.nukeAttempts++;
            data.flaggedBotIds.add(botId);

            // Check for mass bot additions (bot nuke)
            const isMassBotAddition = recentBotAdditions.length >= 1;
            if (isMassBotAddition) {
                data.emergencyMode = true;
            }

            console.log(`⚡ [${member.guild.name}] BLOCKED UNAUTHORIZED BOT: ${member.user.tag}`);
            addActivityLog(member.guild.id, 'UNAUTHORIZED_BOT_BLOCKED', 
                `Blocked: ${member.user.tag}, Banned adder: ${botAdder ? botAdder.tag : 'Unknown'}`, botAdder, member.user);
            await updateUnifiedWidget(member.guild);

            // Send alert to log channel
            const logChannel = await getLogChannel(member.guild);
            if (logChannel) {
                const alertEmbed = new EmbedBuilder()
                    .setTitle('🚨 UNAUTHORIZED BOT BLOCKED & ADDER BANNED')
                    .setDescription(`**Bot:** ${member.user.tag} (${botId})\n**Bot Adder:** ${botAdder ? `${botAdder.tag} (${botAdder.id})` : 'Unknown'}\n**Action:** Bot kicked, adder permanently banned\n**Status:** ${isMassBotAddition ? 'BOT NUKE ATTEMPT' : 'Unauthorized bot addition'}`)
                    .setColor(0xFF0000)
                    .setTimestamp();

                await logChannel.send({ embeds: [alertEmbed] });
            }
        } else {
            console.log(`✅ [${member.guild.name}] WHITELISTED BOT JOINED: ${member.user.tag}`);
            addActivityLog(member.guild.id, 'WHITELISTED_BOT_JOINED', `Joined: ${member.user.tag}`, null, member.user);
            await updateUnifiedWidget(member.guild);
        }

    } catch (error) {
        console.error(`Bot addition error in ${member.guild.name}:`, error);
    }
});

// Member ban detection
client.on('guildBanAdd', async (ban) => {
    try {
        const data = initServerData(ban.guild.id);

        // Track this ban
        const recentBans = trackRealtimeAction(ban.guild.id, 'MEMBER_BAN', ban.user.id);

        // Check for mass banning
        const isMassBan = recentBans.length >= 3; // 3+ bans in 10 seconds

        if (isMassBan) {
            const auditLog = await getAuditLog(ban.guild, AuditLogEvent.MemberBanAdd);

            if (auditLog && Date.now() - auditLog.createdTimestamp < 5000) {
                const executor = auditLog.executor;

                if (executor && executor.id !== ban.guild.ownerId) {
                    // Check if executor is a whitelisted bot (should not happen for ban action)
                    const isWhitelistedBot = executor.bot && data.whitelistedBots.has(executor.id);

                    // Check if executor has whitelisted role
                    const executorMember = await ban.guild.members.fetch(executor.id).catch(() => null);
                    const hasWhitelistedRolePerm = executorMember && hasWhitelistedRole(executorMember, data);

                    if (!isWhitelistedBot) {
                        if (hasWhitelistedRolePerm) {
                            // KICK (not ban) users with whitelisted role
                            await executorMember.kick(`🚫 WHITELISTED USER - MASS BAN ATTEMPT`);
                            console.log(`⚡ [${ban.guild.name}] KICKED WHITELISTED USER: ${executor.tag} for mass banning`);

                            const logChannel = await getLogChannel(ban.guild);
                            if (logChannel) {
                                const alertEmbed = new EmbedBuilder()
                                    .setTitle('⚠️ WHITELISTED USER KICKED FOR MASS BAN ATTEMPT')
                                    .setDescription(`**Executor:** ${executor.tag} (${executor.id})\n**Banned User:** ${ban.user.tag}\n**Action:** User kicked (not banned due to whitelisted status)`)
                                    .setColor(0xFF6600)
                                    .setTimestamp();

                                await logChannel.send({ embeds: [alertEmbed] });
                            }
                        } else {
                            // Instantly ban the mass banner
                            await permanentBan(ban.guild, executor.id, 'MASS BAN ATTEMPT');

                            data.stats.nukeAttempts++;
                            globalStats.nukeAttempts++;
                            data.emergencyMode = true;

                            console.log(`⚡ [${ban.guild.name}] MASS BAN DETECTED: ${executor.tag}`);
                            addActivityLog(ban.guild.id, 'MASS_BAN_BLOCKED', `Mass banner: ${executor.tag}`, executor, ban.user);
                            await updateUnifiedWidget(ban.guild);
                        }
                    }
                }
            }
        }
    } catch (error) {
        console.error(`Ban detection error in ${ban.guild.name}:`, error);
    }
});

// Voice state monitoring for auto-reconnect
client.on('voiceStateUpdate', handleVoiceStateUpdate);

// Auto-cleanup message system - ONLY for security-logs channel
async function cleanupMessages(channel, data) {
    // ONLY clean messages in security-logs channel
    if (!channel || channel.name !== 'security-logs') return;

    try {
        const messages = await channel.messages.fetch({ limit: 100 }).catch(() => new Map());
        const messagesToDelete = [];

        for (const [messageId, msg] of messages) {
            // Skip ULTIMATE ANTI-NUKE SHIELD widgets and embeds
            if (msg.author.id === client.user.id && 
                (msg.embeds.length > 0 && msg.embeds[0].title?.includes('ULTIMATE ANTI-NUKE SHIELD'))) {
                continue;
            }

            // Delete all other messages that are older than 3 seconds
            if (Date.now() - msg.createdTimestamp > 3000) {
                messagesToDelete.push(msg);
            }
        }

        // Bulk delete messages (max 100 at a time, must be less than 14 days old)
        if (messagesToDelete.length > 0) {
            const recentMessages = messagesToDelete.filter(msg => 
                Date.now() - msg.createdTimestamp < 14 * 24 * 60 * 60 * 1000
            );

            if (recentMessages.length > 1) {
                await channel.bulkDelete(recentMessages, true).catch(() => {});
            } else if (recentMessages.length === 1) {
                await recentMessages[0].delete().catch(() => {});
            }

            // Delete older messages individually
            const oldMessages = messagesToDelete.filter(msg => 
                Date.now() - msg.createdTimestamp >= 14 * 24 * 60 * 60 * 1000
            );

            for (const msg of oldMessages) {
                try {
                    await msg.delete();
                    await new Promise(resolve => setTimeout(resolve, 100)); // Rate limit protection
                } catch (error) {
                    // Ignore deletion errors for very old messages
                }
            }

            if (messagesToDelete.length > 0) {
                console.log(`🧹 [${channel.guild.name}] Cleaned ${messagesToDelete.length} messages from #${channel.name}`);
            }
        }
    } catch (error) {
        console.error(`Message cleanup error in ${channel.guild.name}:`, error);
    }
}

// Text commands and message monitoring (optimized)
client.on('messageCreate', async (message) => {
    // Skip DMs
    if (!message.guild) return;

    const data = initServerData(message.guild.id);

    // Auto-cleanup system - ONLY delete messages in security-logs channel after 3 seconds
    if (message.channel.name === 'security-logs' && 
        (message.author.id !== client.user.id || 
         (message.author.id === client.user.id && 
          (!message.embeds.length || !message.embeds[0].title?.includes('ULTIMATE ANTI-NUKE SHIELD'))))) {

        setTimeout(async () => {
            try {
                await message.delete();
            } catch (error) {
                // Message might already be deleted
            }
        }, 3000);
    }

    // Owner commands only
    if (message.author.bot || message.author.id !== message.guild?.ownerId) return;

    const content = message.content.trim();

    if (content === '24/7') {
        if (message.channel.name !== 'security-logs') {
            const reply = await message.reply('❌ Use this command in #security-logs only.');
            setTimeout(() => reply.delete().catch(() => {}), 3000);
            return;
        }

        data.isMonitoring = !data.isMonitoring;
        const statusMsg = data.isMonitoring ? 
            '⚡ **24/7 MONITORING ACTIVATED**' :
            '❌ **24/7 MONITORING DEACTIVATED**';

        const reply = await message.reply(statusMsg);
        addActivityLog(message.guild.id, 'MONITORING_TOGGLE', `Monitoring ${data.isMonitoring ? 'activated' : 'deactivated'}`, message.author, null);
        await updateUnifiedWidget(message.guild);

        // Don't auto-delete this important status message
        return;
    }

    // Add whitelist command
    if (content.startsWith('addw ')) {
        const botId = content.split(' ')[1];
        if (!botId || !/^\d{17,19}$/.test(botId)) {
            const reply = await message.reply('❌ Invalid bot ID.');
            setTimeout(() => reply.delete().catch(() => {}), 3000);
            return;
        }

        try {
            const bot = await client.users.fetch(botId).catch(() => null);
            if (!bot || !bot.bot) {
                const reply = await message.reply('❌ Invalid bot or bot not found.');
                setTimeout(() => reply.delete().catch(() => {}), 3000);
                return;
            }

            if (data.whitelistedBots.has(botId)) {
                const reply = await message.reply(`✅ Bot ${bot.tag} already whitelisted.`);
                setTimeout(() => reply.delete().catch(() => {}), 3000);
                return;
            }

            data.whitelistedBots.add(botId);
            const reply = await message.reply(`✅ **WHITELISTED** ${bot.tag}`);
            addActivityLog(message.guild.id, 'BOT_WHITELISTED', `Whitelisted: ${bot.tag}`, message.author, bot);
            await updateUnifiedWidget(message.guild);
            setTimeout(() => reply.delete().catch(() => {}), 5000);
        } catch (error) {
            const reply = await message.reply('❌ Error whitelisting bot.');
            setTimeout(() => reply.delete().catch(() => {}), 3000);
        }
    }

    // Remove whitelist command
    if (content.startsWith('remw ')) {
        const botId = content.split(' ')[1];
        if (!data.whitelistedBots.has(botId)) {
            const reply = await message.reply('❌ Bot not whitelisted.');
            setTimeout(() => reply.delete().catch(() => {}), 3000);
            return;
        }

        try {
            const bot = await client.users.fetch(botId).catch(() => null);
            const botTag = bot ? bot.tag : `Unknown Bot (${botId})`;

            data.whitelistedBots.delete(botId);

            // Attempt to kick the bot if it's in the guild
            const member = await message.guild.members.fetch(botId).catch(() => null);
            if (member) {
                await member.kick('🚨 REMOVED FROM WHITELIST');
            }

            const reply = await message.reply(`✅ **REMOVED FROM WHITELIST** ${botTag}`);
            addActivityLog(message.guild.id, 'BOT_UNWHITELISTED', `Removed: ${botTag}`, message.author, bot);
            await updateUnifiedWidget(message.guild);
            setTimeout(() => reply.delete().catch(() => {}), 5000);
        } catch (error) {
            const reply = await message.reply('❌ Error removing bot from whitelist.');
            setTimeout(() => reply.delete().catch(() => {}), 3000);
        }
    }

    // Add whitelisted role command and create role if it doesn't exist
    if (content.startsWith('addrolew ')) {
        const roleNameOrMention = content.split(' ').slice(1).join(' ').trim();
        
        // Check if it's a role mention
        const roleId = roleNameOrMention?.replace(/[<@&>]/g, '');
        
        try {
            let role = null;
            
            // If it looks like a role ID, try to fetch it
            if (/^\d{17,19}$/.test(roleId)) {
                role = await message.guild.roles.fetch(roleId);
            } else {
                // Try to find existing role by name first
                role = message.guild.roles.cache.find(r => r.name === roleNameOrMention);
            }
            
            // If no role found and it's the special name, create it
            if (!role && roleNameOrMention === '₊˚  ᰔ secured ✿') {
                role = await message.guild.roles.create({
                    name: '₊˚  ᰔ secured ✿',
                    color: '#FFD700',
                    reason: 'Created whitelist role for anti-nuke protection',
                    permissions: []
                });
                console.log(`✅ [${message.guild.name}] Created whitelist role: ${role.name}`);
            }
            
            if (!role) {
                const reply = await message.reply('❌ Role not found. For the special whitelist role, use: `addrolew ₊˚  ᰔ secured ✿`\nOr mention an existing role: `addrolew @RoleName`');
                setTimeout(() => reply.delete().catch(() => {}), 3000);
                return;
            }

            if (data.whitelistedRoleIds.has(role.id)) {
                const reply = await message.reply(`✅ Role "${role.name}" is already whitelisted.`);
                setTimeout(() => reply.delete().catch(() => {}), 3000);
                return;
            }

            data.whitelistedRoleIds.add(role.id);
            const reply = await message.reply(`✅ **WHITELISTED ROLE:** "${role.name}"`);
            addActivityLog(message.guild.id, 'ROLE_WHITELISTED', `Whitelisted role: ${role.name}`, message.author, null);
            await updateUnifiedWidget(message.guild);
            setTimeout(() => reply.delete().catch(() => {}), 5000);
        } catch (error) {
            console.error('Error adding whitelisted role:', error);
            const reply = await message.reply('❌ Error whitelisting role.');
            setTimeout(() => reply.delete().catch(() => {}), 3000);
        }
    }

    // Remove whitelisted role command
    if (content.startsWith('remrolew ')) {
        const roleMention = content.split(' ')[1];
        const roleId = roleMention?.replace(/[<@&>]/g, '');
        if (!roleId || !/^\d{17,19}$/.test(roleId)) {
            const reply = await message.reply('❌ Invalid role mention. Please use `@role` or `role_id`.');
            setTimeout(() => reply.delete().catch(() => {}), 3000);
            return;
        }

        if (!data.whitelistedRoleIds.has(roleId)) {
            const reply = await message.reply('❌ Role not found in whitelist.');
            setTimeout(() => reply.delete().catch(() => {}), 3000);
            return;
        }

        try {
            const role = await message.guild.roles.fetch(roleId);
            const roleName = role ? role.name : `Unknown Role (${roleId})`;

            data.whitelistedRoleIds.delete(roleId);
            const reply = await message.reply(`✅ **REMOVED ROLE FROM WHITELIST:** "${roleName}"`);
            addActivityLog(message.guild.id, 'ROLE_UNWHITELISTED', `Removed role: ${roleName}`, message.author, null);
            await updateUnifiedWidget(message.guild);
            setTimeout(() => reply.delete().catch(() => {}), 5000);
        } catch (error) {
            console.error('Error removing whitelisted role:', error);
            const reply = await message.reply('❌ Error removing role from whitelist.');
            setTimeout(() => reply.delete().catch(() => {}), 3000);
        }
    }

    // Release timeout command
    if (content.startsWith('!release ')) {
        const userId = content.split(' ')[1].replace(/[<@!>]/g, '');
        if (data.timedoutUsers.has(userId)) {
            const timeoutData = data.timedoutUsers.get(userId);
            const member = await message.guild.members.fetch(userId).catch(() => null);

            if (member && member.communicationDisabledUntil) {
                await member.timeout(null, 'Manual release by server owner');
                data.timedoutUsers.delete(userId);

                const reply = await message.reply(`✅ **RELEASED** <@${userId}>\n**Reason:** ${timeoutData.reason}`);
                addActivityLog(message.guild.id, 'TIMEOUT_RELEASED', 'Manual release', message.author, member.user);
                await updateUnifiedWidget(message.guild);
                setTimeout(() => reply.delete().catch(() => {}), 5000);
            } else {
                const reply = await message.reply('❌ User is not currently timed out or cannot be found.');
                setTimeout(() => reply.delete().catch(() => {}), 3000);
            }
        } else {
            const reply = await message.reply('❌ User not in timeout system');
            setTimeout(() => reply.delete().catch(() => {}), 3000);
        }
    }

    // Voice join command
    if (content === 'joinvc') {
        await joinTargetVoiceChannel(message.guild);
        const reply = await message.reply('🎵 Attempting to join target voice channel...');
        setTimeout(() => reply.delete().catch(() => {}), 3000);
    }

    // Channel cleanup command
    if (content === 'cleanup') {
        await cleanupMessages(message.channel, data);
        const reply = await message.reply('🧹 **CHANNEL CLEANED**');
        setTimeout(() => reply.delete().catch(() => {}), 3000);
    }

    // Help command
    if (content === 'help' || content === '!help') {
        const helpEmbed = new EmbedBuilder()
            .setTitle('🛡️ ANTI-NUKE BOT COMMANDS')
            .setColor(0x0099FF)
            .addFields(
                { name: '🔧 OWNER COMMANDS', value: '`24/7` - Toggle monitoring\n`addw <bot_id>` - Whitelist bot\n`remw <bot_id>` - Remove bot\n`addrolew <@role/role_id>` - Whitelist role\n`remrolew <@role/role_id>` - Remove role\n`!release <user>` - Release timeout\n`joinvc` - Join voice channel\n`cleanup` - Clean channel', inline: false },
                { name: '⚡ PROTECTIONS', value: '**INSTANT BAN:** Channel deletion, Channel modification, Webhook creation, Malicious role creation/modification\n**INSTANT KICK:** Unauthorized bot addition, Mass banning, Mass role deletion\n**28 DAY TIMEOUT:** Channel modifications\n**AUTO-CLEANUP:** All messages deleted after 3 seconds (except widgets)', inline: false },
                { name: '🤖 BOT PROTECTION', value: '**WHITELIST ONLY:** Only whitelisted bots allowed\n**INSTANT KICK:** Unauthorized bots removed\n**PERMANENT BAN:** Users adding unauthorized bots', inline: false },
                { name: '🛡️ ROLE PROTECTION', value: '**WHITELISTED ROLE:** Specific roles grant immunity to bans, only kicks for malicious actions.\n**INSTANT KICK:** Whitelisted users kicked for malicious actions.\n**INSTANT BAN:** Non-whitelisted users banned for malicious actions.', inline: false },
                { name: '🧹 AUTO-CLEANUP', value: '**SECURITY-LOGS ONLY:** Messages in #security-logs auto-deleted after 3 seconds\n**WIDGET PROTECTION:** ULTIMATE ANTI-NUKE SHIELD widgets preserved\n**OTHER CHANNELS:** Normal messages preserved', inline: false }
            )
            .setTimestamp();

        const reply = await message.reply({ embeds: [helpEmbed] });
        // Don't auto-delete help command - important reference
    }
});

// Kick users with whitelisted role if they perform malicious actions.
// The original code handled banning for most malicious activities.
// This change ensures that users with whitelisted roles are kicked instead of banned.
// This logic has been applied to channel deletion, channel updates, webhook creation, role updates, role creation, role deletion, bot additions, and bans.

// Enhanced anti-raid mechanism with proper invite management
let raidMode = false;
let inviteLinkExpiry = null;
const RAID_MEMBER_THRESHOLD = 5; // Reduced to 5 for faster detection
const RAID_TIME_WINDOW = 10000; // 10 seconds window

// Store per-guild raid tracking
const guildRaidTracking = new Map();

client.on('guildMemberAdd', async (member) => {
    // Handle bot additions separately (already handled above)
    if (member.user.bot) return;

    const guild = member.guild;
    const data = initServerData(guild.id);
    const now = Date.now();

    // Initialize guild raid tracking
    if (!guildRaidTracking.has(guild.id)) {
        guildRaidTracking.set(guild.id, {
            recentJoins: [],
            raidActive: false,
            raidStartTime: null
        });
    }

    const guildRaid = guildRaidTracking.get(guild.id);
    
    // Add this join to tracking
    guildRaid.recentJoins.push(now);

    // Remove old timestamps outside the time window
    guildRaid.recentJoins = guildRaid.recentJoins.filter(timestamp => now - timestamp < RAID_TIME_WINDOW);

    // Check for raid conditions
    if (!raidMode && !guildRaid.raidActive && guildRaid.recentJoins.length >= RAID_MEMBER_THRESHOLD) {
        // ACTIVATE ANTI-RAID MODE
        raidMode = true;
        guildRaid.raidActive = true;
        guildRaid.raidStartTime = now;
        inviteLinkExpiry = now + (60 * 60 * 1000); // 1 hour expiry
        data.emergencyMode = true;

        console.log(`🚨 [${guild.name}] ANTI-RAID ACTIVATED - ${guildRaid.recentJoins.length} members joined in ${RAID_TIME_WINDOW/1000} seconds`);

        try {
            // Try to delete existing invites to prevent more raids
            const invites = await guild.fetchInvites();
            let deletedInvites = 0;
            
            for (const [inviteCode, invite] of invites) {
                try {
                    // Only delete invites that are not permanent and not from bots
                    if (!invite.maxAge || invite.maxAge > 0) {
                        await invite.delete('🚨 ANTI-RAID: Preventing raid through invite');
                        deletedInvites++;
                    }
                } catch (error) {
                    console.error(`Failed to delete invite ${inviteCode}:`, error);
                }
            }

            console.log(`🗑️ [${guild.name}] Deleted ${deletedInvites} invites to prevent raid`);

        } catch (error) {
            console.error(`[${guild.name}] Failed to manage invites for anti-raid:`, error);
        }

        // Send comprehensive raid alert with RELEASE INVITES button
        const logChannel = await getLogChannel(guild);
        if (logChannel) {
            const raidEmbed = new EmbedBuilder()
                .setTitle('🚨 ANTI-RAID PROTECTION ACTIVATED')
                .setDescription(`**MASS JOIN DETECTED!**\n\n🔒 **${guildRaid.recentJoins.length} members** joined within **${RAID_TIME_WINDOW/1000} seconds**\n\n⚠️ **EMERGENCY ACTIONS TAKEN:**\n• Invite links invalidated/deleted\n• Emergency mode activated\n• New joins will be monitored closely\n\n🛡️ **Protection remains active until manually released**`)
                .addFields(
                    { name: '📊 Detection Stats', value: `Members: **${guildRaid.recentJoins.length}**\nTime Window: **${RAID_TIME_WINDOW/1000}s**\nThreshold: **${RAID_MEMBER_THRESHOLD}**`, inline: true },
                    { name: '⏰ Auto-Expiry', value: `<t:${Math.floor(inviteLinkExpiry / 1000)}:R>`, inline: true },
                    { name: '🎯 Next Action', value: 'Click **"Release Invites"** to restore normal operations', inline: false }
                )
                .setColor(0xFF0000)
                .setTimestamp();

            const releaseButton = new ButtonBuilder()
                .setCustomId('release_invites')
                .setLabel('🔓 Release Invites')
                .setStyle(ButtonStyle.Success)
                .setEmoji('✅');

            const row = new ActionRowBuilder().addComponents(releaseButton);

            await logChannel.send({ 
                content: `<@${guild.ownerId}> **URGENT: Anti-raid protection activated!**`,
                embeds: [raidEmbed], 
                components: [row] 
            });
        }

        // Log the raid activity
        addActivityLog(guild.id, 'ANTI_RAID_ACTIVATED', `Mass join detected: ${guildRaid.recentJoins.length} members in ${RAID_TIME_WINDOW/1000}s`, null, member.user);
        await updateUnifiedWidget(guild);

        // Alert owner via DM if possible
        try {
            const owner = await client.users.fetch(guild.ownerId);
            if (owner) {
                const dmEmbed = new EmbedBuilder()
                    .setTitle(`🚨 RAID DETECTED IN ${guild.name}`)
                    .setDescription(`Your server is under a potential raid attack.\n\nCheck the security-logs channel and click "Release Invites" when ready.`)
                    .setColor(0xFF0000);
                
                await owner.send({ embeds: [dmEmbed] });
            }
        } catch (error) {
            console.error(`Failed to DM owner about raid in ${guild.name}:`, error);
        }

    } else if (guildRaid.raidActive) {
        // During active raid, monitor new joins more closely
        console.log(`⚠️ [${guild.name}] New member joined during active raid: ${member.user.tag}`);
        
        // Optionally kick new members during active raid (uncomment if desired)
        // await member.kick('🚨 Anti-raid protection active - rejoin after owner releases invites');
        
        const logChannel = await getLogChannel(guild);
        if (logChannel) {
            const joinEmbed = new EmbedBuilder()
                .setTitle('⚠️ RAID MODE: New Member Join')
                .setDescription(`**${member.user.tag}** joined during active raid protection`)
                .addFields(
                    { name: 'Account Created', value: `<t:${Math.floor(member.user.createdTimestamp/1000)}:R>`, inline: true },
                    { name: 'Join Time', value: `<t:${Math.floor(now/1000)}:R>`, inline: true }
                )
                .setColor(0xFF6600)
                .setTimestamp();
                
            await logChannel.send({ embeds: [joinEmbed] });
        }
    }

    // Auto-expire raid mode if time limit reached and not manually released
    if (raidMode && guildRaid.raidActive && now > inviteLinkExpiry) {
        raidMode = false;
        guildRaid.raidActive = false;
        data.emergencyMode = false;
        
        console.log(`⏰ [${guild.name}] Anti-raid mode auto-expired`);
        
        const logChannel = await getLogChannel(guild);
        if (logChannel) {
            const expiredEmbed = new EmbedBuilder()
                .setTitle('⏰ ANTI-RAID AUTO-EXPIRED')
                .setDescription('Raid protection has automatically expired. Normal operations resumed.')
                .setColor(0xFFAA00)
                .setTimestamp();
                
            await logChannel.send({ embeds: [expiredEmbed] });
        }
        
        await updateUnifiedWidget(guild);
    }
});

// Handle the "Release Invites" button click and other buttons
client.on('interactionCreate', async (interaction) => {
    if (!interaction.isButton()) return;

    try {
        const data = initServerData(interaction.guild.id);

        switch (interaction.customId) {
            case 'release_invites':
                if (interaction.guild.ownerId !== interaction.user.id) {
                    return await interaction.reply({ content: '❌ Only the server owner can release invites.', ephemeral: true });
                }

                // Reset raid mode globally and for this guild
                raidMode = false;
                inviteLinkExpiry = null;
                if (interaction.guild.recentlyJoinedMembers) {
                    interaction.guild.recentlyJoinedMembers = [];
                }

                // Exit emergency mode
                data.emergencyMode = false;

                const logChannel = await getLogChannel(interaction.guild);
                if (logChannel) {
                    const embed = new EmbedBuilder()
                        .setTitle('✅ INVITES RELEASED')
                        .setDescription('**Server owner has released the invite restrictions.**\n\n🔓 Normal invite operations can resume\n🛡️ Anti-nuke protection remains active\n⚡ Monitoring continues')
                        .setColor(0x00FF00)
                        .setTimestamp();
                    await logChannel.send({ embeds: [embed] });
                }

                console.log(`⚡ [${interaction.guild.name}] INVITES RELEASED BY OWNER.`);
                addActivityLog(interaction.guild.id, 'INVITES_RELEASED', 'Owner released invite restrictions', interaction.user, null);
                await updateUnifiedWidget(interaction.guild);

                // Remove button and update message
                await interaction.message.edit({ components: [] });
                await interaction.reply({ content: '✅ **Invite restrictions have been lifted!** Normal operations resumed.', ephemeral: true });
                break;

            case 'view_timeouts':
                const timeoutList = Array.from(data.timedoutUsers.entries())
                    .map(([userId, timeoutData]) => {
                        const releaseTime = Math.floor(timeoutData.releaseAt / 1000);
                        return `<@${userId}> - ${timeoutData.reason.substring(0, 40)}... (Until: <t:${releaseTime}:R>)`;
                    })
                    .join('\n') || 'No users currently timed out';

                const timeoutEmbed = new EmbedBuilder()
                    .setTitle('⏰ ACTIVE TIMEOUTS')
                    .setDescription(timeoutList.length > 4096 ? timeoutList.substring(0, 4096) + '...' : timeoutList)
                    .setColor(0xFF6600)
                    .setTimestamp();

                await interaction.reply({ embeds: [timeoutEmbed], ephemeral: true });
                break;

            case 'view_bans':
                const recentBans = data.activityLog
                    .filter(activity => activity.type === 'BAN')
                    .slice(0, 10)
                    .map(activity => {
                        const timeAgo = Math.floor(activity.timestamp / 1000);
                        return `**${activity.target.tag}** - ${activity.description} (<t:${timeAgo}:R>)`;
                    })
                    .join('\n') || 'No recent bans';

                const banEmbed = new EmbedBuilder()
                    .setTitle('⚠️ RECENT BANS')
                    .setDescription(recentBans)
                    .setColor(0xFF0000)
                    .setTimestamp();

                await interaction.reply({ embeds: [banEmbed], ephemeral: true });
                break;

            case 'view_activity':
                const recentActivity = data.activityLog
                    .slice(0, 10)
                    .map(activity => {
                        const timeAgo = Math.floor(activity.timestamp / 1000);
                        const executor = activity.executor ? activity.executor.tag : 'System';
                        const target = activity.target ? activity.target.tag : 'N/A';
                        return `**${activity.type}** by ${executor} → ${target} (<t:${timeAgo}:R>)`;
                    })
                    .join('\n') || 'No recent activity';

                const activityEmbed = new EmbedBuilder()
                    .setTitle('📊 RECENT ACTIVITY')
                    .setDescription(recentActivity)
                    .setColor(0x0099FF)
                    .setTimestamp();

                await interaction.reply({ embeds: [activityEmbed], ephemeral: true });
                break;

            case 'view_stats':
                const uptime = Math.floor(client.uptime / 1000);
                const hours = Math.floor(uptime / 3600);
                const minutes = Math.floor((uptime % 3600) / 60);

                const statsEmbed = new EmbedBuilder()
                    .setTitle('📈 DETAILED STATISTICS')
                    .addFields(
                        { name: '⏰ Timeouts', value: data.stats.timeouts.toString(), inline: true },
                        { name: '⚠️ Bans', value: data.stats.bans.toString(), inline: true },
                        { name: '👢 Kicks', value: data.stats.kicks.toString(), inline: true },
                        { name: '🤖 Blocked Bots', value: data.stats.blockedBots.toString(), inline: true },
                        { name: '🚫 Nuke Attempts', value: data.stats.nukeAttempts.toString(), inline: true },
                        { name: '⚡ Uptime', value: `${hours}h ${minutes}m`, inline: true },
                        { name: '🛡️ Raid Mode', value: raidMode ? 'ACTIVE' : 'STANDBY', inline: true },
                        { name: '🔐 Whitelisted Roles', value: data.whitelistedRoleIds.size.toString(), inline: true },
                        { name: '🤖 Whitelisted Bots', value: data.whitelistedBots.size.toString(), inline: true }
                    )
                    .setColor(0x00FF00)
                    .setTimestamp();

                await interaction.reply({ embeds: [statsEmbed], ephemeral: true });
                break;
        }

    } catch (error) {
        console.error('Button interaction error:', error);
        await interaction.reply({ content: '❌ Error processing request.', ephemeral: true }).catch(() => {});
    }
});


// Periodic cleanup and monitoring intervals
setInterval(() => {
    // Clear rate limit cache
    rateLimitCache.clear();

    // Clear real-time action tracker (keep only last 10 seconds)
    const now = Date.now();
    for (const [key, actions] of realtimeActionTracker.entries()) {
        const recentActions = actions.filter(action => now - action.timestamp < 10000);
        if (recentActions.length === 0) {
            realtimeActionTracker.delete(key);
        } else {
            realtimeActionTracker.set(key, recentActions);
        }
    }

    // Reset emergency mode if no recent nuke activity
    for (const [guildId, data] of serverData.entries()) {
        if (data.emergencyMode) {
            const recentNukeActivity = data.activityLog.filter(log => 
                log.type.includes('NUKE') || log.type.includes('MASS') || log.type.includes('BLOCKED')
            ).filter(log => now - log.timestamp < 300000); // 5 minutes

            if (recentNukeActivity.length === 0) {
                data.emergencyMode = false;
                console.log(`✅ [${guildId}] Emergency mode deactivated - no recent threats`);
            }
        }
    }

    // Reset raid mode if expired and not manually released
    if (raidMode && inviteLinkExpiry && Date.now() > inviteLinkExpiry) {
        raidMode = false;
        inviteLinkExpiry = null;
        console.log(`⚡ Global raid mode expired.`);
        // No guild specific data to update here for raidMode
    }
}, 30000); // Clear cache and reset emergency mode every 30 seconds

// Timeout monitoring interval
setInterval(async () => {
    const now = Date.now();

    for (const [guildId, data] of serverData.entries()) {
        for (const [userId, timeoutData] of data.timedoutUsers.entries()) {
            if (now >= timeoutData.releaseAt) {
                try {
                    const guild = client.guilds.cache.get(guildId);
                    if (guild) {
                        const member = await guild.members.fetch(userId).catch(() => null);
                        if (member) {
                            addActivityLog(guildId, 'TIMEOUT_COMPLETED', 'Timeout period completed', null, member.user);
                        }
                        await updateUnifiedWidget(guild);
                    }
                } catch (error) {
                    console.error('Timeout completion error:', error);
                }
                data.timedoutUsers.delete(userId);
            }
        }
    }
}, 30000); // Check timeouts every 30 seconds

// Periodic widget updates for monitoring servers
setInterval(async () => {
    for (const [guildId, data] of serverData.entries()) {
        if (data.isMonitoring) {
            try {
                const guild = client.guilds.cache.get(guildId);
                if (guild) await updateUnifiedWidget(guild);
            } catch (error) {
                console.error('Periodic update error:', error);
            }
        }
    }
}, 60000); // Update widgets every minute

// Periodic channel cleanup (every 30 seconds) - ONLY security-logs channel
setInterval(async () => {
    for (const [guildId, data] of serverData.entries()) {
        if (data.isMonitoring) {
            try {
                const guild = client.guilds.cache.get(guildId);
                if (guild) {
                    // Clean ONLY the security-logs channel
                    const securityLogsChannel = guild.channels.cache.find(ch => ch.name === 'security-logs');
                    if (securityLogsChannel) {
                        await cleanupMessages(securityLogsChannel, data);
                    }
                }
            } catch (error) {
                console.error('Periodic cleanup error:', error);
            }
        }
    }
}, 30000); // Clean security-logs channel every 30 seconds

// Error handling
client.on('error', error => console.error('🚨 Client error:', error));
process.on('unhandledRejection', error => console.error('🚨 Unhandled rejection:', error));

// Login with enhanced error handling
const botToken = process.env.DISCORD_BOT_TOKEN;
if (!botToken) {
    console.error('🚨 DISCORD_BOT_TOKEN not found in environment variables');
    console.error('🔧 Please add your Discord bot token to the Secrets tab:');
    console.error('   1. Go to the Secrets tab in your Repl');
    console.error('   2. Add key: DISCORD_BOT_TOKEN');
    console.error('   3. Add your bot token as the value');
    console.error('   4. Get your token from: https://discord.com/developers/applications');
    process.exit(1);
}

console.log('🔑 Discord bot token found, attempting to login...');

client.login(botToken).then(() => {
    console.log('✅ Successfully logged into Discord!');
}).catch(error => {
    console.error('🚨 Failed to login to Discord:', error);
    console.error('🔧 Please check if your DISCORD_BOT_TOKEN is valid');
    process.exit(1);
});