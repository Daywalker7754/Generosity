import logging

from ConfigHandler import ConfigHandler
from src.BookingStatementHandler import BookingStatementHandler
from src.ImportHandler import ImportHandler

logging.basicConfig(level=logging.DEBUG, filename='Main.log')
# logging.basicConfig(level=logging.ERROR)

# Get the Instances
imp = ImportHandler()
config = ConfigHandler()

# Importieren des Kapitalflussberichts - aktuell nur manuell
import_filename = config.get_statement_of_funds_name()
imp.import_ib_xml_manual(import_filename)

# Erstellen der Buchungssätze
accounts_to_process = config.get_ib_accounts()
account_mapping = config.get_ib_to_accounting_map()
bookings = BookingStatementHandler(accounts_to_process, account_mapping)

accounts_to_combine = config.get_ib_acc_combination()
bookings.generate_booking_journal(accounts_to_combine)
