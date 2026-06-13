"""
DKSC Pipeline Control v1.1
CustomTkinter reconstruction of the Bloomberg-style Dukascopy pipeline terminal.

Static 800x600 desktop window. The FEED ticker streams live market data from
the Alpha Vantage API (real quotes, uppercase tickers, no hard-coded values).

Run:
    pip install -r requirements.txt
    python main.py
"""

from dksc.app import DKSCTerminal


def main():
    app = DKSCTerminal()
    app.mainloop()


if __name__ == "__main__":
    main()
