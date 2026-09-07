const COPY_ERROR = "Copy failed. Select the command and copy it manually.";

export function bindInstallCopy(root, clipboard) {
  root.querySelectorAll("[data-install-copy]").forEach((container) => {
    const button = container.querySelector("button");
    const code = container.querySelector("code");
    const status = container.querySelector('[role="status"]');
    let copying = false;

    button.hidden = false;
    button.addEventListener("click", async () => {
      if (copying) return;
      const command = code.textContent;
      status.textContent = "";
      if (!clipboard || typeof clipboard.writeText !== "function") {
        status.textContent = COPY_ERROR;
        return;
      }

      copying = true;
      button.setAttribute("aria-busy", "true");
      try {
        await clipboard.writeText(command);
        status.textContent = "Copied!";
      } catch {
        status.textContent = COPY_ERROR;
      } finally {
        copying = false;
        button.removeAttribute("aria-busy");
      }
    });
  });
}
