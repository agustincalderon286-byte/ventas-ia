import mongoose from 'mongoose';
import { openToken, sealToken } from './coach-hycite-gmail.js';
import { waNumber } from './coach-wa-chat-input.js';

const schema = new mongoose.Schema({
  _id: mongoose.Schema.Types.ObjectId,
  encryptedPhone: { type: String, required: true }
}, { versionKey: false, bufferCommands: false });
const Store = mongoose.models.CoachWhatsAppProfile || mongoose.model('CoachWhatsAppProfile', schema);
const phoneKey = owner => 'coach-whatsapp-sender:' + owner;

export function createWaProfile({ key, store = Store }) {
  return {
    async load(owner) {
      const doc = await store.findById(owner, 'encryptedPhone').lean();
      return doc ? openToken(doc.encryptedPhone, key, phoneKey(owner)) : '';
    },
    async save(owner, rawPhone) {
      const phone = typeof rawPhone === 'string' && rawPhone.length <= 50 && waNumber(rawPhone);
      if (!phone) return false;
      const encryptedPhone = sealToken(phone, key, phoneKey(owner));
      await store.updateOne({ _id: owner }, { $set: { encryptedPhone } }, { upsert: true });
      return true;
    },
    async remove(owner) {
      await store.deleteOne({ _id: owner });
    }
  };
}
