# TEC Chef: private appointment-request pilot

Release: `tec-chef-20261110-demo1`.

## User scope

Keep `/coach/chef/` restricted to TEC administrators during testing. On
2026-11-10 the user explicitly requested removing the passkey requirement from
Chef so they can demonstrate it. Chef now checks the ordinary authenticated TEC
session and administrator allowlist for its page, assets and API; it does not
reuse the Torre step-up guard. Torre and all other administrative controls retain
their existing passkey verification. WhatsApp, public lead intake, automatic
campaigns and confirmed calendar bookings remain disabled.

The logo has intrinsic dimensions and critical fallback styles. A loading recovery
panel links to normal TEC sign-in and retry. No Chef page link directs users to
passkey enrollment. Regression tests exercise an administrator with an unverified
session, actual CSS/scripts/mascot delivery, chat and lead capture, while rejecting
visitors and ordinary members. Existing Torre/passkey tests still run unchanged.

The robot illustration and virtual-phone layout accompany the existing Spanish
recipe, product, care and warranty assistant. The WebP mascot is approximately
77 KB. Its generated original and prompt are retained in the task review folder.

## Appointment request

The assistant offers **Solicitar cita** for commercial intent; the same action is
available below the conversation. Five local questions collect name, telephone
or email, city, product and preferred time. These answers stay in the page's
memory and do not go into the AI request or saved AI history. This is an online
pilot; leaving or reloading an unsubmitted request discards that draft.

A review and explicit authorization checkbox precede **Enviar solicitud**.
`POST /api/chef/admin/leads` validates everything again, fixes ownership to the
authenticated administrator and saves a `CoachLeadInbox` record. Preferred time
is a preference, never a reserved appointment. The server fixes status to
`solicitada`; client-supplied ownership, delivery address and status are ignored.

## Notification

The existing `createTecMailer` service sends the notification only to
`agustincalderon286@gmail.com`, through the configured Hostinger mailbox or the
existing Resend configuration. No new key or provider is required. The subject
marks this as a private test. The notification includes contact details, product,
city and preferred time, without the full AI conversation.

`TecChefLeadNotification` stores a hashed account/request key and delivery state.
An atomic claim prevents concurrent duplicate sends. A configuration failure can
be retried safely; a timeout or ambiguous delivery is marked `unknown` and is
not blindly sent again. The lead remains stored if delivery fails. Provider
acceptance is reported as `sent`; it is not proof of inbox delivery. The UI never
reports successful email delivery when configuration is absent or uncertain.

The notification is attempted after saving the lead. It is not a background
delivery queue: a server interruption between those steps needs a retry with the
same request ID. Public rollout should add operator visibility/reconciliation
for pending, unknown or abandoned sending states before unattended intake.

## Verification

Tests cover admin access, existing Chef knowledge/history, the production chat
controller, five guided answers, explicit consent, contact-data isolation,
validation, fixed recipient, owner isolation, immutable requested status,
concurrent posts, idempotent retries, provider timeout, missing configuration and
the existing mail adapters. No real customer data or real appointments were
created by automated tests.

Live browser control is disabled in the current Work settings. Actual visual
review on iPhone/iPad and receipt of a new Chef notification are therefore still
manual checks. The existing shared production email connection had a verified
support-message receipt on 2026-11-07; this does not substitute for a new Chef
end-to-end inbox check. Unauthenticated HTTP checks verify release and continued
private access after deployment.
