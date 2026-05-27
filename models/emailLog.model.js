const mongoose = require('mongoose');

const emailLogSchema = new mongoose.Schema({
    to: { type: String, required: true },
    subject: String,
    template: String,
    status: { type: String, enum: ['sent', 'failed', 'pending', 'retrying'], default: 'pending' },
    error: String,
    attempts: { type: Number, default: 0 },
    sentAt: Date,
    metadata: mongoose.Schema.Types.Mixed
}, { timestamps: true });

// Check if model already compiled to prevent mongoose OverwriteModelError in server runtime restarts
module.exports = mongoose.models.EmailLog || mongoose.model('EmailLog', emailLogSchema);
