// dark-mode.js
document.addEventListener("DOMContentLoaded", () => {
    const toggle = document.querySelector("[data-dark-toggle]");
    const body = document.body;

    // Load saved preference
    const stored = localStorage.getItem("vt_dark_mode");
    if (stored === "true") {
        body.classList.add("dark");
        if (toggle) toggle.textContent = "☀️ Light";
    }

    if (toggle) {
        toggle.addEventListener("click", () => {
            body.classList.toggle("dark");
            const isDark = body.classList.contains("dark");
            localStorage.setItem("vt_dark_mode", isDark);
            toggle.textContent = isDark ? "☀️ Light" : "🌙 Dark";
        });
    }
});