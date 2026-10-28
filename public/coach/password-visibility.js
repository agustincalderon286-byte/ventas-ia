(() => {
  document.querySelectorAll("[data-password-toggle]").forEach(button => {
    const input = document.getElementById(button.getAttribute("aria-controls"));
    if (!input || input.type !== "password") return;

    const setVisible = visible => {
      input.type = visible ? "text" : "password";
      const label = visible ? "Ocultar contraseña" : "Mostrar contraseña";
      button.setAttribute("aria-pressed", String(visible));
      button.setAttribute("aria-label", label);
      button.title = label;
    };

    button.hidden = false;
    button.addEventListener("click", () => {
      const start = input.selectionStart;
      const end = input.selectionEnd;
      setVisible(input.type === "password");
      input.focus({ preventScroll: true });
      if (start !== null && end !== null) input.setSelectionRange(start, end);
    });
    input.form?.addEventListener("submit", () => setVisible(false));
    window.addEventListener("pageshow", () => setVisible(false));
  });
})();
