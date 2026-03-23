const faqCards = document.querySelectorAll(".faq-card");
const placeholderButtons = document.querySelectorAll("[data-placeholder-action]");
const coachForms = document.querySelectorAll(".coach-form");

faqCards.forEach(card => {
  const trigger = card.querySelector(".faq-trigger");

  if (!trigger) {
    return;
  }

  trigger.addEventListener("click", () => {
    card.classList.toggle("is-open");
  });
});

placeholderButtons.forEach(button => {
  button.addEventListener("click", event => {
    event.preventDefault();

    const target = button.getAttribute("data-placeholder-action") || "continuar";
    const message =
      target === "checkout"
        ? "La pantalla ya quedó lista. El siguiente paso es conectar Stripe para que este botón abra el pago real."
        : "La interfaz ya quedó lista. El siguiente paso es conectar el login real y la suscripción activa.";

    window.alert(message);
  });
});

coachForms.forEach(form => {
  form.addEventListener("submit", event => {
    event.preventDefault();

    const result = form.querySelector(".form-result");

    if (result) {
      result.classList.add("is-visible");
    }
  });
});
