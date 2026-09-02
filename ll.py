import ctypes
import ctypes.wintypes
import time
import argparse
import signal
import sys


ES_CONTINUOUS = 0x80000000
ES_DISPLAY_REQUIRED = 0x00000002


def set_display_required():
    ctypes.windll.kernel32.SetThreadExecutionState(ES_CONTINUOUS | ES_DISPLAY_REQUIRED)


def clear_display_required():
    ctypes.windll.kernel32.SetThreadExecutionState(ES_CONTINUOUS)


class POINT(ctypes.Structure):
    _fields_ = [("x", ctypes.c_long), ("y", ctypes.c_long)]


def get_cursor_pos():
    pt = POINT()
    ctypes.windll.user32.GetCursorPos(ctypes.byref(pt))
    return pt.x, pt.y


def set_cursor_pos(x, y):
    ctypes.windll.user32.SetCursorPos(int(x), int(y))


def run(loop_interval: float = 30.0, wiggle: bool = True):
    running = True

    def handle_sigint(signum, frame):
        nonlocal running
        running = False

    signal.signal(signal.SIGINT, handle_sigint)
    signal.signal(signal.SIGTERM, handle_sigint)

    try:
        # initial request to keep display on
        set_display_required()
        while running:
            # periodically re-assert the execution state
            set_display_required()

            if wiggle:
                x, y = get_cursor_pos()
                # tiny move and back
                set_cursor_pos(x + 1, y)
                time.sleep(0.05)
                set_cursor_pos(x, y)

            time.sleep(loop_interval)
    finally:
        # clear the display-required flag when exiting
        clear_display_required()


def main():
    parser = argparse.ArgumentParser(description="Evita que a tela seja desligada movendo o mouse/solicitando display.")
    parser.add_argument("--interval", "-i", type=float, default=30.0, help="Intervalo em segundos entre ações (default: 30)")
    parser.add_argument("--no-wiggle", dest="wiggle", action="store_false", help="Desativa o movimento do mouse (só usa API de energia)")
    args = parser.parse_args()

    try:
        run(loop_interval=args.interval, wiggle=args.wiggle)
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
