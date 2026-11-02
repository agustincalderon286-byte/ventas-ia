export function hologramFrame(seconds, intensity = .9) {
  const t = Math.max(0, seconds), reveal = Math.min(1, t / 1.8);
  const ease = 1 - (1 - reveal) ** 3;
  return { reveal: ease, height: .08 + ease * .13 + Math.sin(t * 1.8) * .022 * ease,
    rotation: t * .22, scan: (t * .23) % 1, pulse: .75 + Math.sin(t * 2.7) * .14,
    energy: Math.max(.35, Math.min(1.4, intensity)) };
}
