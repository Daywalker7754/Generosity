import logging
import os

from ConfigHandler import ConfigHandler
from src.BookingStatementHandler import BookingStatementHandler
from src.ImportHandler import ImportHandler

logging.basicConfig(level=logging.DEBUG, filename='Main.log')
# logging.basicConfig(level=logging.ERROR)

# Get the Instances
imp = ImportHandler()
config = ConfigHandler()

# Importieren des Kapitalflussberichts - aktuell nur manuell
print(os.getcwd())
import_filename = config.get_statement_of_funds_name()
open_position_filename = config.get_file_open_positions_name()
print(import_filename)
imp.import_ib_xml_manual(import_filename)

for key in config.get_ib_accounts():
    imp.import_open_position(key, "Backup_OpenPositions.xlsx")

for key in open_position_filename:
    imp.import_open_position(key, open_position_filename[key])

# Erstellen der Buchungssätze
accounts_to_process = config.get_ib_accounts()
account_mapping = config.get_ib_to_accounting_map()
start_date = config.get_start_date()
end_date = config.get_end_date()
bookings = BookingStatementHandler(accounts_to_process, account_mapping, start_date, end_date)

accounts_to_combine = config.get_ib_acc_combination()
bookings.generate_booking_journal(accounts_to_combine)
