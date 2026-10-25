import 'dotenv/config';
import mongoose from 'mongoose';
import { publishCoachKnowledge } from '../src/knowledge/coach-knowledge.js';
try {
  await mongoose.connect(process.env.MONGODB_URI, { serverSelectionTimeoutMS: 10000 });
  console.log(JSON.stringify(await publishCoachKnowledge(mongoose.connection.db)));
} catch (error) {
  console.error('TEC knowledge publication failed:', error.name);
  process.exitCode = 1;
} finally { await mongoose.disconnect(); }
