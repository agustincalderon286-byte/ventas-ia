// Each draft has exactly one explicit recipient. Display names and address lists
// are intentionally not interpreted as mailboxes.
export function isGmailAddress(address) {
  if (typeof address !== 'string' || address.length > 254 || /[\s\x00-\x1f\x7f]/u.test(address)) return false;
  const parts = address.split('@');
  if (parts.length !== 2 || parts[0].length > 64) return false;
  const local = /^[a-z\d!#$%&'*+\/=?^_`{|}~-]+(?:\.[a-z\d!#$%&'*+\/=?^_`{|}~-]+)*$/i;
  const label = /^[a-z\d](?:[a-z\d-]{0,61}[a-z\d])?$/i;
  const domain = parts[1].split('.');
  return local.test(parts[0]) && domain.length >= 2 && domain.every(part => label.test(part));
}
