/**
 * Dedicated Standalone Background Worker for Email Queues
 */
const dotenv = require('dotenv');
const path = require('path');
const mongoose = require('mongoose');
const { Resend } = require('resend');
const crypto = require('crypto');
const { Redis } = require('ioredis');

// Load environment variables from the local .env file
dotenv.config();

const EmailLog = require('./models/emailLog.model');

const WORKER_ID = `worker-${crypto.randomBytes(6).toString("hex")}`;
const MAX_ATTEMPTS = 4;

console.log("==========================================");
console.log("   BOOTING STANDALONE EMAIL WORKER NODE   ");
console.log("==========================================");

// Validate Env
if (!process.env.MONGO_URI) throw new Error("MONGO_URI is required");

let redisSubscriber = null;

// Retry Utils
class TimeoutError extends Error {
    constructor(message = "Request timed out") {
        super(message);
        this.name = "TimeoutError";
    }
}

function calculateBackoffDelay(attempt, baseDelayMs = 2000, maxDelayMs = 30000) {
    const exponential = Math.pow(2, attempt - 1);
    const delay = Math.min(baseDelayMs * exponential, maxDelayMs);
    const jitterFactor = 0.8 + Math.random() * 0.4;
    return Math.floor(delay * jitterFactor);
}

function withTimeout(promise, timeoutMs = 15000) {
    let timeoutId;
    const timeoutPromise = new Promise((_, reject) => {
        timeoutId = setTimeout(() => {
            reject(new TimeoutError(`Connection timed out after ${timeoutMs / 1000} seconds`));
        }, timeoutMs);
    });
    return Promise.race([
        promise.then((res) => {
            clearTimeout(timeoutId);
            return res;
        }),
        timeoutPromise
    ]);
}

function isRetryableError(error) {
    if (!error) return false;
    const message = (error.message || "").toLowerCase();
    if (message.includes("invalid") || message.includes("validation") || message.includes("unauthorized") || message.includes("api key")) {
        return false;
    }
    return (
        error.name === "TimeoutError" ||
        message.includes("429") || message.includes("rate limit") || message.includes("timeout") ||
        message.includes("network") || message.includes("econnreset") || message.includes("500")
    );
}

let cachedResend = null;
let cachedApiKey = "";

function getResendClient(apiKey) {
    if (cachedResend && cachedApiKey === apiKey) {
        return cachedResend;
    }
    cachedResend = new Resend(apiKey);
    cachedApiKey = apiKey;
    return cachedResend;
}

class EmailQueueWorker {
    constructor() {
        this.activeQueue = new Set();
        this.isProcessing = false;
        this.workerInterval = null;
    }

    start() {
        console.log(`⚙️ [Email Queue] Background worker [${WORKER_ID}] started.`);
        this.sweepPendingJobs();
        this.workerInterval = setInterval(() => {
            this.sweepPendingJobs();
        }, 3000); // Poll MongoDB every 3 seconds for near-instant fallback sending
    }

    stop() {
        if (this.workerInterval) clearInterval(this.workerInterval);
        console.log(`🔌 [Email Queue] Background worker [${WORKER_ID}] halted.`);
    }

    enqueue(dbLogId) {
        if (!dbLogId) return;
        setImmediate(() => {
            this.processJob(dbLogId).catch((err) => {
                console.error(`[Email Queue] Critical failure processing job ${dbLogId}:`, err.message);
            });
        });
    }

    async processJob(dbLogId) {
        if (this.activeQueue.has(dbLogId.toString())) return;
        this.activeQueue.add(dbLogId.toString());

        let dbLog = null;
        try {
            const fiveMinutesAgo = new Date(Date.now() - 5 * 60 * 1000);
            dbLog = await EmailLog.findOneAndUpdate(
                {
                    _id: dbLogId,
                    status: { $in: ["pending", "retrying"] },
                    $or: [
                        { "metadata.lockedAt": null },
                        { "metadata.lockedAt": { $lt: fiveMinutesAgo } }
                    ]
                },
                {
                    $set: {
                        "metadata.lockedAt": new Date(),
                        "metadata.lockedBy": WORKER_ID
                    }
                },
                { returnDocument: 'after' }
            );

            if (!dbLog) {
                this.activeQueue.delete(dbLogId.toString());
                return;
            }

            dbLog.attempts = (dbLog.attempts || 0) + 1;
            await dbLog.save();

            let fromName = "Inistnt";
            let fromAddress = process.env.EMAIL_FROM;
            let resendApiKey = process.env.RESEND_API_KEY;
            let replyToAddress = "";

            try {
                // Fetch from settings collection directly via mongoose connection db
                const nameDoc = await mongoose.connection.db.collection('settings').findOne({ key: 'mail_from_name' });
                if (nameDoc && nameDoc.value) fromName = nameDoc.value;

                const addressDoc = await mongoose.connection.db.collection('settings').findOne({ key: 'mail_from_address' });
                if (addressDoc && addressDoc.value) fromAddress = addressDoc.value;

                const keyDoc = await mongoose.connection.db.collection('settings').findOne({ key: 'resend_api_key' });
                if (keyDoc && keyDoc.value) resendApiKey = keyDoc.value;

                const replyDoc = await mongoose.connection.db.collection('settings').findOne({ key: 'mail_reply_to' });
                if (replyDoc && replyDoc.value) replyToAddress = replyDoc.value;
            } catch (err) {
                console.warn("[Email Worker] Failed to load mail settings from DB:", err.message);
            }

            // Fallback to environment variables if not set in DB
            fromAddress = fromAddress || process.env.EMAIL_FROM;
            resendApiKey = resendApiKey || process.env.RESEND_API_KEY;

            if (!resendApiKey) {
                throw new Error("RESEND_API_KEY is not defined (neither in DB settings nor in Env).");
            }
            if (!fromAddress) {
                throw new Error("EMAIL_FROM is not defined (neither in DB settings nor in Env).");
            }

            // Extract email address cleanly
            let cleanAddress = fromAddress;
            if (fromAddress && fromAddress.includes("<")) {
                const match = fromAddress.match(/<([^>]+)>/);
                if (match) {
                    cleanAddress = match[1];
                }
            }

            const resolvedFrom = `"${fromName}" <${cleanAddress}>`;

            const resend = getResendClient(resendApiKey);

            const payload = {
                from: resolvedFrom,
                to: [dbLog.to],
                subject: dbLog.subject,
                html: dbLog.metadata?.htmlBody || `<p>${dbLog.subject}</p>`
            };

            if (replyToAddress) {
                payload.reply_to = replyToAddress;
            } else if (process.env.EMAIL_REPLY_TO) {
                payload.reply_to = process.env.EMAIL_REPLY_TO;
            }

            const sendPromise = resend.emails.send(payload);
            const response = await withTimeout(sendPromise, 15000);

            if (response.error) {
                throw new Error(response.error.message || `Resend Error: ${JSON.stringify(response.error)}`);
            }

            dbLog.status = "sent";
            dbLog.sentAt = new Date();
            dbLog.metadata.resendId = response.data?.id; // Keep resendId for compatibility with backend logs UI
            await this.releaseLock(dbLog, "sent");
            console.log(`✅ [Email Queue] Sent email to ${dbLog.to} via Resend (MessageID: ${response.data?.id})`);

        } catch (error) {
            console.error(`⚠️ [Email Queue] Delivery failed for job <${dbLogId}>:`, error.message);
            if (dbLog) await this.handleJobFailure(dbLog, error);
        } finally {
            this.activeQueue.delete(dbLogId.toString());
        }
    }

    async handleJobFailure(dbLog, error) {
        const canRetry = isRetryableError(error) && dbLog.attempts < MAX_ATTEMPTS;

        if (canRetry) {
            const delayMs = calculateBackoffDelay(dbLog.attempts);
            dbLog.error = error.message;
            await this.releaseLock(dbLog, "retrying");
            
            setTimeout(() => {
                this.enqueue(dbLog._id);
            }, delayMs);
            console.log(`⏳ [Email Queue] Retrying job <${dbLog._id}> in ${Math.round(delayMs/1000)}s`);
        } else {
            dbLog.error = error.message;
            await this.releaseLock(dbLog, "failed");
            console.log(`❌ [Email Queue] Job <${dbLog._id}> failed permanently.`);
        }
    }

    async releaseLock(dbLog, finalStatus = null) {
        try {
            dbLog.metadata.lockedAt = null;
            dbLog.metadata.lockedBy = null;
            if (finalStatus) dbLog.status = finalStatus;
            await EmailLog.findByIdAndUpdate(dbLog._id, { 
                $set: { 
                    status: dbLog.status, 
                    error: dbLog.error, 
                    sentAt: dbLog.sentAt,
                    attempts: dbLog.attempts,
                    "metadata.lockedAt": null, 
                    "metadata.lockedBy": null,
                    "metadata.resendId": dbLog.metadata?.resendId
                } 
            });
        } catch (err) {
            console.error(`[Email Queue] Failed to release lock on job ${dbLog._id}:`, err.message);
        }
    }

    async sweepPendingJobs() {
        if (this.isProcessing) return;
        this.isProcessing = true;
        try {
            const fiveMinutesAgo = new Date(Date.now() - 5 * 60 * 1000);
            const staleOrPending = await EmailLog.find({
                status: { $in: ["pending", "retrying"] },
                attempts: { $lt: MAX_ATTEMPTS },
                $or: [
                    { "metadata.lockedAt": null },
                    { "metadata.lockedAt": { $lt: fiveMinutesAgo } }
                ]
            }).limit(20);

            if (staleOrPending.length > 0) {
                console.log(`[Email Queue] Sweeper recovered ${staleOrPending.length} pending email jobs to process.`);
                for (const job of staleOrPending) {
                    this.enqueue(job._id);
                }
            }
        } catch (error) {
            console.error("[Email Queue] Sweeper execution failure:", error.message);
        } finally {
            this.isProcessing = false;
        }
    }
}

const worker = new EmailQueueWorker();

async function start() {
    try {
        await mongoose.connect(process.env.MONGO_URI, { maxPoolSize: 5, serverSelectionTimeoutMS: 5000 });
        console.log("✅ MongoDB Connected");

        if (process.env.REDIS_URL) {
            redisSubscriber = new Redis(process.env.REDIS_URL);
            redisSubscriber.on('ready', () => {
                console.log("✅ Redis connected and ready for subscription");
                redisSubscriber.subscribe("email:jobs", (err) => {
                    if (err) console.error("❌ Redis Subscribe Error:", err);
                });
            });
            redisSubscriber.on("message", (channel, dbLogId) => {
                if (channel === "email:jobs") {
                    worker.enqueue(dbLogId);
                }
            });
        }

        worker.start();
    } catch (err) {
        console.error("❌ Worker Initialization Failed:", err);
        process.exit(1);
    }
}

start();

process.on('SIGTERM', async () => {
    worker.stop();
    await mongoose.connection.close();
    if (redisSubscriber) await redisSubscriber.quit();
    process.exit(0);
});
process.on('SIGINT', async () => {
    worker.stop();
    await mongoose.connection.close();
    if (redisSubscriber) await redisSubscriber.quit();
    process.exit(0);
});
