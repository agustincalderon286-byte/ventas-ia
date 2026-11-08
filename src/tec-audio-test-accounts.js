import crypto from 'node:crypto';
import { promisify } from 'node:util';

const scrypt = promisify(crypto.scrypt);
export const normalizeTestUsername = value => typeof value === 'string'
  ? value.trim().normalize('NFD').replace(/\p{Diacritic}/gu, '').toLowerCase() : '';
export const validTestUsername = value => /^[a-z][a-z0-9_-]{2,31}$/.test(value);
export const hasTestAudioAccess = user => Boolean(user?.isTest === true && user.analyticsExcluded === true &&
  user.testAccessEnabled === true && user.accessType === 'test' && user.credentialVersion);

export async function createTestPassword(password) {
  const passwordSalt = crypto.randomBytes(16).toString('hex');
  const passwordHash = (await scrypt(password, passwordSalt, 64)).toString('hex');
  return { passwordSalt, passwordHash };
}

export async function verifyTestPassword(password, user) {
  // Equal work for unknown users; only database-provisioned test accounts can sign in here.
  const salt = /^[a-f0-9]{32}$/.test(user?.passwordSalt || '') ? user.passwordSalt : '0'.repeat(32);
  const expected = /^[a-f0-9]{128}$/.test(user?.passwordHash || '') ? user.passwordHash : '0'.repeat(128);
  const actual = await scrypt(password, salt, 64);
  return crypto.timingSafeEqual(actual, Buffer.from(expected, 'hex')) && hasTestAudioAccess(user);
}
