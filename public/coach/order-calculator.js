// Pure calculator shared by the on-screen totals and the scenario sent to Coach.
// Amounts are calculated in cents. These are existing estimates, not financing terms.
(() => {
  "use strict";

  function calculateOrder({ products = [], summary = {} } = {}) {
    const errors = [];
    const fail = (scope, field, message) => errors.push({ scope, field, message });
    const read = (raw, scope, field, label, percent = false) => {
      const text = String(raw ?? "").trim();
      const value = text === "" ? 0 : Number(text);
      const scaled = value * 100;
      if ((text && !/^[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:e[+-]?\d+)?$/i.test(text)) ||
          !Number.isFinite(value) || value < 0 || !Number.isSafeInteger(Math.round(scaled))) {
        fail(scope, field, `${label}: escribe un número válido mayor o igual a cero.`);
        return 0;
      }
      if (percent && value > 100) {
        fail(scope, field, `${label}: el porcentaje debe estar entre 0 y 100.`);
        return 0;
      }
      if (Math.abs(scaled - Math.round(scaled)) > 0.000001) {
        fail(scope, field, `${label}: usa como máximo dos decimales.`);
        return 0;
      }
      return Math.round(scaled);
    };
    const safeSum = (...values) => {
      const value = values.reduce((sum, amount) => sum + amount, 0);
      if (!Number.isSafeInteger(value)) {
        fail("summary", "", "Los importes son demasiado grandes. Revisa los precios antes de continuar.");
        return 0;
      }
      return value;
    };
    const fraction = (cents, numerator, denominator) => Number(
      (BigInt(cents) * BigInt(numerator) + BigInt(denominator / 2)) / BigInt(denominator)
    );
    const payments = balance => {
      // Keep the original formula: monthly = 5% of balance, weekly = monthly / 4,
      // daily = weekly / 7. Round each displayed estimate independently to cents.
      return { monthly: fraction(balance, 5, 100) / 100,
        weekly: fraction(balance, 5, 400) / 100,
        daily: fraction(balance, 5, 2800) / 100 };
    };

    let totalGross = 0, totalDown = 0, totalDiscount = 0;
    const calculated = products.map((product, index) => {
      const base = read(product.basePrice, index, "basePrice", "Precio");
      const down = read(product.down, index, "down", "Anticipo");
      const fixed = read(product.descFixed, index, "descFixed", "Descuento fijo");
      const percent = read(product.descPercent, index, "descPercent", "Descuento", true);
      const tax = fraction(base, 10, 100);
      const shipping = fraction(base, 5, 100);
      const gross = safeSum(base, tax, shipping);
      const discount = safeSum(fixed, fraction(gross, percent, 10000));
      if (safeSum(down, discount) > gross) {
        ["down", "descFixed", "descPercent"].forEach(field => fail(index, field,
          `Producto ${index + 1}: el anticipo y los descuentos juntos no pueden superar el total bruto.`));
      }
      const balance = Math.max(0, gross - down - discount);
      totalGross = safeSum(totalGross, gross);
      totalDown = safeSum(totalDown, down);
      totalDiscount = safeSum(totalDiscount, discount);
      return { slot: index + 1, name: String(product.name || "").trim(), code: String(product.code || "").trim(),
        basePrice: base / 100, tax: tax / 100, shipping: shipping / 100, grossTotal: gross / 100,
        downPayment: down / 100, discountAmount: discount / 100, balance: balance / 100, ...payments(balance) };
    });

    const extraDown = read(summary.extraDown, "summary", "extraDown", "Anticipo extra");
    const extraFixed = read(summary.extraDescFixed, "summary", "extraDescFixed", "Descuento fijo total");
    const extraPercent = read(summary.extraDescPercent, "summary", "extraDescPercent", "Descuento total", true);
    const finalDown = safeSum(totalDown, extraDown);
    // A general percentage still applies to total gross, as in the original tool.
    const finalDiscount = safeSum(totalDiscount, extraFixed, fraction(totalGross, extraPercent, 10000));
    if (safeSum(finalDown, finalDiscount) > totalGross) {
      ["extraDown", "extraDescFixed", "extraDescPercent"].forEach(field => fail("summary", field,
        "El total de anticipos y descuentos supera el pedido. Revisa los valores por producto y los extras."));
    }
    if (errors.length) return { valid: false, errors, products: [], summary: null };
    const balance = totalGross - finalDown - finalDiscount;
    return { valid: true, errors: [], products: calculated, summary: {
      totalGross: totalGross / 100, totalDown: finalDown / 100, totalDiscount: finalDiscount / 100,
      balanceFinal: balance / 100, ...payments(balance), extraDown: extraDown / 100,
      extraDescFixed: extraFixed / 100, extraDescPercent: extraPercent / 100
    } };
  }

  window.TecOrderCalculator = Object.freeze({ calculateOrder });
})();
