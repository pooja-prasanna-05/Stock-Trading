// navbar.js
document.addEventListener("DOMContentLoaded", () => {
    const menuToggle = document.querySelector("[data-menu-toggle]");
    const menu = document.querySelector("[data-menu]");

    if (menuToggle && menu) {
        menuToggle.addEventListener("click", () => {
            const visible = menu.style.display === "flex";
            menu.style.display = visible ? "none" : "flex";
        });
    }
});