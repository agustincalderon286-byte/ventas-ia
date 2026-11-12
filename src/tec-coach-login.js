// A phone is only a username, not a verified contact or SMS recovery method.
// Existing email login remains unchanged; provisioned US phone seats use 10 digits.
export function parseCoachLogin(input) {
  if (typeof input !== 'string' || input.length > 254) return null;
  const value = input.trim().toLowerCase();
  if (/^\S+@\S+\.\S+$/.test(value)) return { field: 'email', value };
  if (!/^\+?[\d\s().-]+$/.test(value)) return null;
  let digits = value.replace(/\D/g, '');
  if (digits.length === 11 && digits.startsWith('1')) digits = digits.slice(1);
  if (!/^[2-9]\d{2}[2-9]\d{6}$/.test(digits)) return null;
  return { field: 'username', value: digits };
}
