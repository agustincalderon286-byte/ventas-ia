#!/usr/bin/env node

import crypto from "crypto";
import mongoose from "mongoose";

function readArg(name) {
  const index = process.argv.indexOf(name);
  return index >= 0 ? String(process.argv[index + 1] || "").trim() : "";
}

function normalizeEmail(value = "") {
  return String(value || "").trim().toLowerCase();
}

function createPassword(password) {
  const salt = crypto.randomBytes(16).toString("hex");
  const hash = crypto.scryptSync(password, salt, 64).toString("hex");
  return { salt, hash };
}

async function main() {
  const mongoUri = String(process.env.MONGODB_URI || "").trim();
  const mode = String(process.argv[2] || "inspect").trim().toLowerCase();

  if (!mongoUri) {
    throw new Error("Falta MONGODB_URI.");
  }

  await mongoose.connect(mongoUri);
  const users = mongoose.connection.collection("coachusers");
  const profiles = mongoose.connection.collection("coachdistributorprofiles");

  if (mode === "inspect") {
    const rows = await users
      .find(
        {},
        {
          projection: {
            name: 1,
            email: 1,
            accountType: 1,
            teamOwnerUserId: 1,
            billingOwnerUserId: 1,
            subscriptionStatus: 1,
            subscriptionActive: 1,
            seatStatus: 1
          }
        }
      )
      .sort({ createdAt: 1 })
      .toArray();

    console.log(
      JSON.stringify(
        rows.map(row => ({
          id: String(row._id),
          name: row.name || "",
          email: row.email || "",
          accountType: row.accountType || "owner",
          teamOwnerUserId: row.teamOwnerUserId ? String(row.teamOwnerUserId) : "",
          billingOwnerUserId: row.billingOwnerUserId ? String(row.billingOwnerUserId) : "",
          subscriptionStatus: row.subscriptionStatus || "inactive",
          subscriptionActive: Boolean(row.subscriptionActive),
          seatStatus: row.seatStatus || "active"
        })),
        null,
        2
      )
    );
    return;
  }

  if (mode !== "upsert") {
    throw new Error("Modo invalido. Usa inspect o upsert.");
  }

  const ownerEmail = normalizeEmail(readArg("--owner-email"));
  const email = normalizeEmail(readArg("--email"));
  const name = readArg("--name");
  const password = String(process.env.COACH_ADMIN_PASSWORD || "");

  if (!ownerEmail || !email || !name || password.length < 8) {
    throw new Error("Faltan owner-email, email, name o COACH_ADMIN_PASSWORD (minimo 8 caracteres).");
  }

  const owner = await users.findOne({ email: ownerEmail });
  if (!owner) {
    throw new Error(`No encontre la cuenta duena ${ownerEmail}.`);
  }

  const now = new Date();
  const passwordRecord = createPassword(password);
  const current = await users.findOne({ email });
  const base = {
    name,
    email,
    passwordHash: passwordRecord.hash,
    passwordSalt: passwordRecord.salt,
    accountType: "leader",
    parentUserId: owner._id,
    billingOwnerUserId: owner._id,
    teamOwnerUserId: owner._id,
    sponsorUserId: owner.sponsorUserId || null,
    sponsorName: owner.sponsorName || "",
    sponsorCommissionRate: Number(owner.sponsorCommissionRate || 0),
    seatStatus: "active",
    officeId: owner.officeId || "",
    territoryId: owner.territoryId || "",
    subscriptionStatus: "team_access",
    subscriptionActive: false,
    updatedAt: now
  };

  if (current) {
    await users.updateOne({ _id: current._id }, { $set: base });
  } else {
    await users.insertOne({ ...base, createdAt: now });
  }

  const admin = await users.findOne({ email });
  await profiles.updateOne(
    { userId: admin._id },
    {
      $set: {
        name,
        email,
        subscriptionStatus: "team_access",
        teamRole: "lider",
        seatLabel: "Administrador",
        updatedAt: now
      },
      $setOnInsert: {
        chefEnabled: true,
        questionsCount: 0,
        coachRepliesCount: 0,
        createdAt: now
      }
    },
    { upsert: true }
  );

  console.log(
    JSON.stringify(
      {
        ok: true,
        id: String(admin._id),
        name: admin.name,
        email: admin.email,
        accountType: admin.accountType,
        ownerEmail,
        teamOwnerUserId: String(admin.teamOwnerUserId),
        subscriptionStatus: admin.subscriptionStatus
      },
      null,
      2
    )
  );
}

main()
  .catch(error => {
    console.error(error.message || error);
    process.exitCode = 1;
  })
  .finally(async () => {
    await mongoose.disconnect().catch(() => null);
  });
