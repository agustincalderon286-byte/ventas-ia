import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";

const css = fs.readFileSync(new URL("../public/coach/styles.css", import.meta.url), "utf8");
const luminance = hex => {
  const rgb = hex.match(/[a-f0-9]{2}/gi).map(value => Number.parseInt(value, 16) / 255)
    .map(value => value <= 0.04045 ? value / 12.92 : ((value + 0.055) / 1.055) ** 2.4);
  return rgb[0] * 0.2126 + rgb[1] * 0.7152 + rgb[2] * 0.0722;
};
const contrast = (foreground, background) => {
  const values = [luminance(foreground), luminance(background)].sort((a, b) => b - a);
  return (values[0] + 0.05) / (values[1] + 0.05);
};

test("light-theme states keep at least 4.5 to 1 contrast on the audited cards and summary backgrounds", () => {
  const lightRules = css.slice(css.indexOf('html:not([data-coach-portal-mode="telemarketing"]) body.coach-app-body'));
  const groups = [
    { selectors: [".daily-prize-result-card.is-main strong", ".daily-prize-result-card.is-copy strong", ".buyer-profile-result-card.is-main strong"], color: "#102b4e" },
    { selectors: [".decision-score-card.is-highlight strong", '.order-calc-match[data-tone="warning"]', '.order-calc-summary-note[data-state="warning"]'], color: "#805300" },
    { selectors: ['.order-calc-summary-note[data-state="success"]'], color: "#15734c" },
    { selectors: [".order-calc-validation"], color: "#b4232f" }
  ];
  for (const group of groups) {
    const rule = lightRules.split("}").find(block => group.selectors.every(selector => block.includes(selector)) && block.includes(`color: ${group.color};`));
    assert.ok(rule, `missing scoped override for ${group.selectors.join(", ")}`);
    assert.match(rule, /html:not\(\[data-coach-portal-mode="telemarketing"\]\)/);
    for (const background of ["#ffffff", "#f6f8fb", "#f8eef0", "#efdfe4"]) {
      assert.ok(contrast(group.color, background) >= 4.5, `${group.color} on ${background}`);
    }
  }
  assert.match(lightRules, /:is\(input, textarea\)::placeholder\s*\{\s*color: #5f7086;\s*opacity: 1;/);
  assert.ok(contrast("#5f7086", "#ffffff") >= 4.5);
  assert.ok(contrast("#5f7086", "#f6f8fb") >= 4.5);
});
