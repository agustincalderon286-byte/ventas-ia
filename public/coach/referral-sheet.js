(() => {
  "use strict";

  const text = value => String(value ?? "").trim();
  const phoneDigits = value => text(value).replace(/\D/g, "");
  const phoneKey = value => phoneDigits(value).replace(/^1(?=\d{10}$)/, "");
  const validPhone = value => /^\+?[\d().\s\p{Pd}]+$/u.test(text(value)) && /^\d{7,15}$/.test(phoneDigits(value));
  function validPhotoPhone(value) {
    const digits = phoneDigits(value);
    if (!validPhone(value)) return false;
    if (text(value).startsWith("+") && !digits.startsWith("1")) {
      return /^[2-9]\d{9,14}$/.test(digits);
    }
    return (digits.length === 10 && !text(value).startsWith("+")) || /^1\d{10}$/.test(digits);
  }
  const hasContent = row => [row.fullName, row.phone, row.address, row.notes, row.city, row.relationship].some(value => text(value));

  function readForm(form) {
    const value = name => text(form.elements.namedItem(name)?.value);
    const referrals = Array.from(form.querySelectorAll("[data-fourteen-referral-row]"), (row, index) => ({
      fullName: value("referralName" + (index + 1)), phone: value("referralPhone" + (index + 1)),
      city: value("referralCity" + (index + 1)), relationship: value("referralRelationship" + (index + 1)),
      address: value("referralAddress" + (index + 1)), notes: value("referralNotes" + (index + 1))
    }));
    return { hostName:value("hostName"), hostPhone:value("hostPhone"), representativeName:value("representativeName"),
      giftSelected:value("giftSelected"), startDate:value("startDate"), referrals:referrals.filter(hasContent) };
  }

  function review(sheet) {
    if (!text(sheet.hostName) || !validPhone(sheet.hostPhone)) return "Revisa el nombre y el teléfono del anfitrión.";
    if (!sheet.referrals.length) return "Agrega al menos un referido.";
    const seen = new Set();
    for (const row of sheet.referrals) {
      if (!text(row.fullName) || !validPhone(row.phone)) return "Cada referido necesita nombre y teléfono. Completa o limpia el renglón pendiente.";
      const key = phoneKey(row.phone);
      if (seen.has(key)) return "Hay dos referidos con el mismo teléfono en esta hoja. Revisa los renglones antes de continuar.";
      seen.add(key);
    }
    return "";
  }

  function csv(sheet) {
    const tickets = sheet.documentType === "tickets";
    const extraColumns = !tickets && sheet.referrals.some(row => row.city || row.relationship);
    const header = tickets
      ? ["Nombre", "Teléfono", "Dirección", "Ciudad o zona", "Relación", "Ubicación en la foto"]
      : ["Anfitrión", "Teléfono anfitrión", "Nombre", "Teléfono", "Dirección", "Notas del referido", "Representante", "Regalo", "Fecha inicio"];
    if (extraColumns) header.push("Ciudad o zona", "Relación con el anfitrión");
    const rows = [header];
    for (const item of sheet.referrals) {
      const row = tickets ? [item.fullName, phoneKey(item.phone), item.address, item.city, item.relationship, item.location] : [
        sheet.hostName, phoneKey(sheet.hostPhone), item.fullName, phoneKey(item.phone), item.address,
        item.notes, sheet.representativeName, sheet.giftSelected, text(sheet.startDate).slice(0, 10)
      ];
      if (extraColumns) row.push(item.city, item.relationship);
      rows.push(row);
    }
    const quote = field => {
      const value = text(field);
      const safe = /^[=+@\-＝＋＠－]/u.test(value) ? "'" + value : value;
      return '"' + safe.replace(/"/g, '""') + '"';
    };
    return "\uFEFF" + rows.map(row => row.map(quote).join(",")).join("\r\n") + "\r\n";
  }

  function download(sheet) {
    const url = URL.createObjectURL(new Blob([csv(sheet)], {type:"text/csv;charset=utf-8"}));
    const link = document.createElement("a");
    link.href = url;
    const name = text(sheet.hostName).replace(/[^\p{L}\p{N}]+/gu, "-").slice(0,60).replace(/^-|-$/g,"");
    link.download = sheet.documentType === "tickets" ? "TEC-boletas.csv" : "TEC-4-en-14" + (name ? "-" + name : "") + ".csv";
    document.body.appendChild(link);
    link.click();
    link.remove();
    setTimeout(() => URL.revokeObjectURL(url), 30000);
  }

  function applyPhoto(form, photo, maxRows, appendRow) {
    if (photo.documentType && photo.documentType !== "referrals") {
      return {applied:false, message:"Esta foto no corresponde a una hoja de referidos 4 en 14."};
    }
    const field = name => form.elements.namedItem(name);
    const manual = readForm(form);
    const conflict = (a,b) => a && b && a !== b;
    if (conflict(manual.hostName.toLocaleLowerCase(),text(photo.hostName).toLocaleLowerCase()) ||
        conflict(phoneKey(manual.hostPhone),phoneKey(photo.hostPhone))) {
      return {applied:false, message:"El anfitrión de la foto difiere del formulario. Revisa el dato o déjalo vacío en la foto para conservar el que ya escribiste."};
    }
    const seen = new Set(manual.referrals.filter(row => row.phone).map(row => phoneKey(row.phone)));
    for (const row of photo.referrals) {
      if (!text(row.fullName) || !validPhone(row.phone)) return {applied:false,message:"Revisa el nombre y teléfono de cada referido seleccionado."};
      const key = phoneKey(row.phone);
      if (seen.has(key)) return {applied:false,message:"Hay un teléfono repetido. Desmarca el renglón repetido en la foto o corrige el número."};
      seen.add(key);
    }
    if (manual.referrals.length + photo.referrals.length > maxRows) {
      return {applied:false,message:"Solo caben " + (maxRows - manual.referrals.length) + " referidos más. Selecciona los que entran y usa otra hoja para el resto."};
    }
    if (!manual.hostName && photo.hostName) field("hostName").value = photo.hostName;
    if (!manual.hostPhone && photo.hostPhone) field("hostPhone").value = photo.hostPhone;
    let index = 1;
    for (const row of photo.referrals) {
      while (field("referralName" + index) && ["Name","Phone","Address","Notes","City","Relationship"].some(k => text(field("referral" + k + index)?.value))) index++;
      if (!field("referralName" + index)) appendRow();
      field("referralName" + index).value = row.fullName;
      field("referralPhone" + index).value = row.phone;
      field("referralAddress" + index).value = row.address || "";
      field("referralCity" + index).value = row.city || "";
      field("referralRelationship" + index).value = row.relationship || "";
      index++;
    }
    form.dispatchEvent(new Event("input", {bubbles:true}));
    return {applied:true,message:"Datos incorporados al formulario."};
  }

  window.TecReferralSheet = {readForm, review, csv, download, applyPhoto, phoneKey, validPhone, validPhotoPhone};
})();
