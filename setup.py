from distutils.core import setup

setup(console=['Main.py'],
      options={
          "py2exe": {
              "unbuffered": True,
              "optimize": 2,
              "includes": ["ConfigHandler", "src.BookingStatementHandler", "src.ImportHandler", "configparser", ]
          }
      })
