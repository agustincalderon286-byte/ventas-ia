# TEC Chef: private appointment-request pilot

Release: `tec-chef-20261110-access1`.

## User scope

Keep `/coach/chef/` restricted to TEC administrators during testing. The existing
administrator/passkey guard still protects the API and Chef assets. Nothing
enables WhatsApp, public lead intake, automatic campaigns or confirmed calendar
bookings.

The HTML route checks the same passkey assurance before sending its page, just
like the Torre route. An enrolled administrator whose verification expired is
redirected to `/coach/seguridad/?volver=chef`. Previously the HTML loaded while
CSS and JavaScript returned 403, leaving the original full-size TEC logo on an
unstyled page. A regression test now requests the actual stylesheet, scripts and
mascot after verification and checks their MIME types. The logo also has intrinsic
dimensions and small critical fallback styles so a failed asset cannot enlarge it
to its original size. A loading recovery panel provides direct access and retry
links even if JavaScript fails to load.

If assurance expires during an already open appointment review, the client shows
a verification link in a separate tab and preserves the current request in memory;
it does not navigate away with an unsent contact. Server authorization for private
assets, data and submissions remains in place.

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
