#!/usr/bin/env node
// Run only by the service operator. No public account creation or payment bypass endpoint.
import crypto from 'node:crypto';
import { readFile, writeFile, mkdir } from 'node:fs/promises';
import path from 'node:path';
import mongoose from 'mongoose';
import { createTestPassword, validTestUsername } from '../src/tec-audio-test-accounts.js';
const command = process.argv[2];
const names = ['Agustin', 'Rigoberto', 'Enrique', 'Debbie'];
const error = message => { throw new Error(message); };

try {
  if (command === 'prepare') {
    const dir = path.resolve(process.argv[3] || error('Indica una carpeta privada fuera del repositorio.'));
    await mkdir(dir, { recursive: true, mode: 0o700 });
    const credentials = [];
    const records = [];
    for (const name of names) {
      const username = name.toLowerCase();
      const password = crypto.randomBytes(15).toString('base64url');
      credentials.push({ name, username, password });
      records.push({ _id: 'test:' + username, username, displayName: name,
        isTest: true, analyticsExcluded: true, accessType: 'test', testAccessEnabled: true,
        ...await createTestPassword(password), credentialVersion: crypto.randomBytes(16).toString('hex') });
    }
    // Never overwrite existing passwords on a retry.
    await writeFile(path.join(dir, 'Credenciales-privadas.json'), JSON.stringify(credentials, null, 2), { flag: 'wx', mode: 0o600 });
    await writeFile(path.join(dir, 'Provisionar-hashes.json'), JSON.stringify(records), { flag: 'wx', mode: 0o600 });
    const url = 'https://totalelitecoaching.com/audiolibros/?acceso=prueba';
    for (const c of credentials) await writeFile(path.join(dir, c.name + '.txt'),
      `${c.name}, aquí puedes escuchar todos los audiolibros de TEC.\n\n${url}\nUsuario: ${c.username}\nContraseña: ${c.password}\n\nTu acceso es gratuito y ya está habilitado. Abre el enlace y pulsa Entrar a la biblioteca con tus datos.\n`, { flag: 'wx', mode: 0o600 });
    console.log('Preparadas cuatro credenciales y mensajes privados. Ninguna cuenta creada todavía.');
  } else if (command === 'apply') {
    let input = '';
    for await (const chunk of process.stdin) { input += chunk; if (input.length > 16_384) error('Entrada demasiado grande.'); }
    input = input.trim();
    const records = JSON.parse(Buffer.from(input, 'base64').toString('utf8'));
    if (!Array.isArray(records) || records.length !== 4) error('Se requieren exactamente las cuatro cuentas autorizadas.');
    for (const [i, row] of records.entries()) {
      if (row.username !== names[i].toLowerCase() || row._id !== 'test:' + row.username || !validTestUsername(row.username) ||
          row.displayName !== names[i] || row.isTest !== true || row.analyticsExcluded !== true || row.accessType !== 'test' || row.testAccessEnabled !== true ||
          !/^[a-f0-9]{128}$/.test(row.passwordHash) || !/^[a-f0-9]{32}$/.test(row.passwordSalt) || !/^[a-f0-9]{32}$/.test(row.credentialVersion)) error('Registro inválido.');
    }
    if (!process.env.MONGODB_URI) error('Falta MONGODB_URI en el servidor.');
    await mongoose.connect(process.env.MONGODB_URI);
    const accounts = mongoose.connection.collection('tecaudiomembers');
    for (const row of records) {
      const existing = await accounts.findOne({ _id: row._id });
      if (existing && JSON.stringify(Object.fromEntries(Object.keys(row).map(k => [k, existing[k]]))) !== JSON.stringify(row)) {
        error('Existe una cuenta diferente; no se sobrescribió ninguna contraseña.');
      }
    }
    for (const row of records) await accounts.updateOne({ _id: row._id }, { $setOnInsert: { ...row, createdAt: new Date(), updatedAt: new Date() } }, { upsert: true });
    console.log(JSON.stringify({ ok: true, accounts: names, free: true, analyticsExcluded: true, stripeCustomersCreated: 0 }));
  } else if (command === 'disable') {
    const username = process.argv[3];
    if (!validTestUsername(username)) error('Usuario inválido.');
    await mongoose.connect(process.env.MONGODB_URI);
    const result = await mongoose.connection.collection('tecaudiomembers').updateOne({ _id: 'test:' + username, isTest: true },
      { $set: { testAccessEnabled: false, credentialVersion: crypto.randomBytes(16).toString('hex'), updatedAt: new Date() } });
    console.log(JSON.stringify({ disabled: result.matchedCount === 1, username }));
  } else error('Usa prepare <carpeta>, apply (base64 por stdin en el servidor) o disable <usuario>.');
} catch (e) {
  // Avoid printing Mongo connection errors, URIs or credential records.
  console.error(e.name === 'MongoServerError' || /Mongo|network|connect/i.test(e.name) ? 'No se pudo completar la operación en la base de datos.' : e.message);
  process.exitCode = 1;
} finally { await mongoose.disconnect(); }
