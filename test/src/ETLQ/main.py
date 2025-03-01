#==============================#            Description            #==============================#

"""Este módulo actúa como el orquestador principal, coordinando la ejecución de los módulos de extracción, transformación, validación de calidad y carga de datos. Su propósito es asegurar un flujo de trabajo estructurado y eficiente, gestionando la secuencia y dependencia de cada proceso dentro del pipeline ETLQ."""

#==============================# Importamos los módulos necesarios #==============================#

from textual.app import App, ComposeResult
from textual.screen import Screen
from textual.containers import Container
from textual.widgets import Header, Footer, Button, Static

class InstallScreen(Screen):
    CSS = """
    Screen {
        align: center middle;
        background: #1c1c1c;
    }
    #message {
        background: #2e2e2e;
        border: round $accent;
        padding: 2;
        text-align: center;
        color: white;
    }
    """
    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)
        yield Static("Instalación iniciada...\nPor favor, espere.", id="message")
        yield Footer()

class InstallApp(App):
    CSS = """
    Screen {
        align: center middle;
        background: #1c1c1c;
    }
    #main-container {
        background: #2e2e2e;
        border: round $accent;
        width: 60%;
        padding: 2;
    }
    #title {
        text-align: center;
        color: white;
        margin-bottom: 1;
    }
    #install_button {
        margin: 1;  /* Se reemplazó '1 auto' por '1' para evitar el error */
        width: 50%;
        align: center middle;
    }
    """
    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)
        yield Container(
            Static("Bienvenido al Instalador Profesional", id="title"),
            Button("INSTALAR", id="install_button"),
            id="main-container"
        )
        yield Footer()

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "install_button":
            self.push_screen(InstallScreen())

if __name__ == "__main__":
    InstallApp().run()
