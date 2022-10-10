import logging
import os
import time

import numpy as np
import pandas as pd

from src.PathHandler import PathHandler

logging.basicConfig(level=logging.ERROR)
debug = False
save_to_excel = True


class BookingStatementHandler:

    def __init__(self, accounts_to_process, account_mapping, start_date, end_date):
        # Zuerst muss ich die Pfade erstellen
        self.dir = PathHandler()
        dir = self.dir.get_working_dir()
        self.pickle_files = os.path.join(dir, "working_files")
        self.dir_pickle_file = os.path.join(dir, "working_files", "cleaned_data.pkl")
        self.dir_excel_backup = os.path.join(dir, "working_files", "ib_statement_prepared.xlsx")
        self.dir_import = os.path.join(dir, "import")
        self.dir_export = os.path.join(dir, "export")

        self.imported_data = pd.read_pickle(self.dir_pickle_file)
        self.modified_data = pd.DataFrame()
        self.fifo_positions = pd.DataFrame()

        self.journal = pd.DataFrame()
        self.processed_entries = pd.DataFrame()
        self.qualitycheck = pd.DataFrame()
        self.account_mapping = account_mapping

        self.start = start_date
        self.end = end_date

        # Quality Check: Hier prüfe ich, ob alle Accounts angegeben wurden
        self.accounts = self.read_accounts()

        for account in self.accounts:
            if account in accounts_to_process:
                pass
            else:
                logging.error(f"The following account ({account})is missing in the config-file, please add!")

    def unique(self, list):
        x = np.array(list)
        return x

    def read_accounts(self):
        ''' Read the accounts which are in the export of IB '''
        account_list = self.imported_data["accountId"]
        account_list = list(set(account_list))

        return account_list

    def delete_selected_fifo_positions(self, open_trades):
        '''
            Einzelne Einträge sind nicht wirklich in der offenen Posten liste benötigt, da es keine
            offenen Positionen darstellt, sondern einzelne Buchungen, die nur abgeschlossen werden müssen
            Die auszuschließenden Objekte sind:
            - DINT =>
            - DIV =>
            - FRTAX =>
            - OFFE =>
            - STAX =>
        '''

        # Basierend auf dem Code
        list_to_exclude = ["DINT", "DIV", "FRTAX", "OFEE", "STAX", "FUT", "CFD", "CINT", "BFEE"]
        try:
            open_trades_cleaned = open_trades[~open_trades["activityCode"].isin(list_to_exclude)]
        except KeyError:
            open_trades_cleaned = open_trades

        # Basierend auf der Category
        list_to_exclude = ["FUT", "CFD"]
        try:
            open_trades_cleaned = open_trades_cleaned[~open_trades_cleaned["assetCategory"].isin(list_to_exclude)]
        except KeyError:
            open_trades_cleaned = open_trades_cleaned

        return open_trades_cleaned

    def track_processing(self, account_id, transactionID, amount, date):
        ''' Speichert die Transaction-ID, die erfolgreich verbucht wurde '''

        dict_to_save = {"account": account_id,
                        "transactionID": transactionID,
                        "date": date,
                        "processedAmount": amount,
                        }

        if self.processed_entries.empty:  # initiale Befüllung
            self.processed_entries = pd.concat([self.processed_entries, pd.DataFrame([dict_to_save])],
                                               ignore_index=True)

        elif self.processed_entries["transactionID"].isin(
                [transactionID]).any().any():  # wenn TA vorhanden, dann check ob doppeltes Datum
            pass

        else:
            self.processed_entries = pd.concat([self.processed_entries, pd.DataFrame([dict_to_save])],
                                               ignore_index=True)
            self.qualitycheck = pd.concat([self.qualitycheck, pd.DataFrame([dict_to_save])], ignore_index=True)

    def book_statement(self, row, id, desc, sdesc, amount, soll, haben, account_id, quality_check_relevant, text=None):
        ''' Definiert den Buchungssatz, damit diese immer gleich aussehen '''

        if text:
            text_to_journal = str(int(row["transactionID"])) + "_" + row["activityCode"] + "_" + row[
                "assetCategory"] + "_" + text
        else:
            text_to_journal = str(int(row["transactionID"])) + "_" + row["activityCode"] + "_" + row[
                "assetCategory"] + "_" + str(int(row["tradeQuantity"])) + "_" + row["symbol"]

        dict = {"Account": account_id,
                "Belegnummer": int(row["transactionID"]),
                "SATZ_ID": id,
                "DESC": desc,
                "SUBDESC": sdesc,
                "DATE": row["date"],
                "SETTLEDATE": row["settleDate"],
                "TEXT": text_to_journal,
                "AMOUNT": abs(amount),
                "SOLL": soll,
                "HABEN": haben,
                "QUALITYREL": quality_check_relevant}

        self.journal = pd.concat([self.journal, pd.DataFrame([dict])], ignore_index=True)
        self.track_processing(account_id, int(row["transactionID"]), row["amount"], row["date"])
        logging.debug(f"Buchungssatz: {dict}")

        return dict

    def add_open_position(self, row):
        ''' Eröffnet manuell eine offene Position '''
        self.fifo_positions = pd.concat([self.fifo_positions, pd.DataFrame([row])], ignore_index=True)
        self.fifo_positions.drop_duplicates(inplace=True)

        logging.debug(f"Ich eröffne die Position {row['transactionID']}")

    def close_open_position(self, open_transactionID):
        ''' Schließt eine offene Position und nimmt diese aus der offenen Posten Liste '''
        # now I need to clear the open position entry
        self.fifo_positions = self.fifo_positions[self.fifo_positions["transactionID"] != open_transactionID]

        logging.debug(f"Ich schließe die Position: {open_transactionID}")

    def calculate_p_l(self, direction, amount_in_depot, amount_based_on_direction):
        ''' Berechnet den Gewinn und Verlust '''

        amount_in_depot = round(amount_in_depot, 8)
        amount_based_on_direction = round(amount_based_on_direction, 8)

        if direction == "BUY":
            result = amount_in_depot - amount_based_on_direction
        elif direction == "BUYTOCLOSESHORT":

            result = amount_in_depot - amount_based_on_direction

        else:
            result = amount_based_on_direction - amount_in_depot
        if result < 0:
            identifier = "l"
        elif result > 0:
            identifier = "p"
        else:
            identifier = "even"

        logging.debug(f"P&L is calculated as follows with the following "
                      f"parameters: trade direction {direction}, amount_based_on_sell: {amount_based_on_direction}, "
                      f"amount_in_depot: {amount_in_depot}, p/l={identifier}, result:{result}")

        return identifier, result

    def close_position_fifo(self, direction, row, open_in_depot, stock_adjustment, restbuchwert, einnahmen,
                            bank_account_id, account_id):
        ''' Schließen von offenen Positionen nach dem FIFO-Prinzip '''

        stocks_to_sell = abs(row["tradeQuantity"])
        amount_to_sell = abs(row["amount"])

        if not einnahmen > 0:
            einnahmen = 0.0

        if not stock_adjustment > 0:
            stock_adjustment = 0.0

        if not restbuchwert > 0:
            restbuchwert = 0.0

        self.track_processing(account_id, int(row["transactionID"]), row["amount"], row["date"])

        for i, row in open_in_depot.iterrows():

            stocks_in_depot_entry = abs(row["tradeQuantity"])

            if stock_adjustment > 0:
                stocks_in_depot_entry = stock_adjustment

            stocks_in_depot_entry_original = abs(row["tradeQuantity"])
            amount_in_depot_entry = abs(row["amount"])
            amount_in_depot_entry_original = abs(row["amount"])
            stock_in_depot_id = int(row["transactionID"])

            if stocks_to_sell == 0:
                pass
            else:
                if stocks_in_depot_entry == stocks_to_sell:

                    # Calculate the values for the p&l calculation and bookings
                    quantity = abs(stocks_in_depot_entry)

                    if einnahmen > 0:
                        amount_to_sell = einnahmen
                    else:
                        einnahmen = amount_to_sell

                    if restbuchwert == 0:
                        restbuchwert = amount_in_depot_entry
                    else:
                        amount_in_depot_entry = restbuchwert

                    amount_to_sell = (amount_to_sell / stocks_to_sell) * quantity

                    # Calculate the values for the p&l calculation and bookings
                    quantity = abs(stocks_to_sell)

                    logging.info(f'{int(row["transactionID"])} / {row["symbol"]} '
                                 f'=> Option 1: Ich habe {stocks_in_depot_entry} Aktien im Depot. '
                                 f'Hier will ich nun die folgende Operation {direction} '
                                 f'mit der Menge {quantity} durchführen. '
                                 f'Diese bewerte ich mit einem Depotwert von {amount_in_depot_entry}, und '
                                 f'dem Verrechnungswert von {amount_to_sell}.')

                    # Adapt the loop
                    stocks_to_sell = 0
                    stocks_in_depot_entry = 0
                    einnahmen = 0
                    restbuchwert = restbuchwert - amount_in_depot_entry

                    logging.info(f'Nach der Durchführung der Berechnung, habe ich noch die folgende Operation: '
                                 f'{direction} mit der Menge von {stocks_to_sell} Aktien. Bei dem aktuellen Eintrag '
                                 f'habe ich noch {stocks_in_depot_entry} im Depot. Der Buchwert beträgt hierbei '
                                 f'{restbuchwert} und die noch offenen Einnahmen belaufen sich noch auf {einnahmen}')

                    if stocks_in_depot_entry == 0.0:
                        self.close_open_position(stock_in_depot_id)

                elif stocks_in_depot_entry < stocks_to_sell:

                    # Calculate the values for the p&l calculation and bookings
                    quantity = abs(stocks_in_depot_entry)

                    if einnahmen > 0:
                        amount_to_sell = einnahmen
                    else:
                        einnahmen = amount_to_sell

                    if restbuchwert == 0:
                        restbuchwert = amount_in_depot_entry
                    else:
                        amount_in_depot_entry = restbuchwert

                    amount_to_sell = (amount_to_sell / stocks_to_sell) * quantity

                    logging.info(f'{int(row["transactionID"])} / {row["symbol"]} '
                                 f'=> Option 2: Ich habe {stocks_in_depot_entry} Aktien im Depot. '
                                 f'Hier will ich nun die folgende Operation {direction} '
                                 f'mit der Menge {quantity} durchführen. '
                                 f'Diese bewerte ich mit einem Depotwert von {amount_in_depot_entry}, und '
                                 f'dem Verrechnungswert von {amount_to_sell}.')

                    # Adapt the loop and baseline
                    stocks_to_sell = stocks_to_sell - quantity
                    stocks_in_depot_entry = stocks_in_depot_entry - quantity
                    stock_adjustment = stocks_in_depot_entry
                    einnahmen = einnahmen - amount_to_sell
                    restbuchwert = restbuchwert - amount_in_depot_entry

                    logging.info(f'Nach der Durchführung der Berechnung, habe ich noch die folgende Operation: '
                                 f'{direction} mit der Menge von {stocks_to_sell} Aktien. Bei dem aktuellen Eintrag '
                                 f'habe ich noch {stocks_in_depot_entry} im Depot. Der Buchwert beträgt hierbei '
                                 f'{restbuchwert} und die noch offenen Einnahmen belaufen sich noch auf {einnahmen}')

                    if stocks_in_depot_entry == 0.0:
                        self.close_open_position(stock_in_depot_id)

                elif stocks_in_depot_entry > stocks_to_sell:

                    # Calculate the values for the p&l calculation and bookings
                    quantity = abs(stocks_to_sell)
                    if restbuchwert > 0:
                        amount_in_depot_entry = restbuchwert

                    amount_in_depot_entry = (amount_in_depot_entry / stocks_in_depot_entry) * stocks_to_sell

                    if einnahmen > 0:
                        amount_to_sell = einnahmen

                    logging.info(f'{int(row["transactionID"])} / {row["symbol"]} => '
                                 f'Option 3: Ich habe {stocks_in_depot_entry} Aktien im Depot. '
                                 f'Hier will ich nun die folgende Operation {direction} '
                                 f'mit der Menge {quantity} durchführen. '
                                 f'Diese bewerte ich mit einem Depotwert von {amount_in_depot_entry}, und '
                                 f'dem Verrechnungswert von {amount_to_sell}.')

                    # Adapt the loop
                    stocks_to_sell = stocks_to_sell - quantity

                    if row["tradeQuantity"] < 0 and quantity > 0:
                        stocks_in_depot_entry = (stocks_in_depot_entry * -1) + quantity
                    else:
                        stocks_in_depot_entry = stocks_in_depot_entry - quantity

                    stock_adjustment = stocks_in_depot_entry

                    # ich habe mehr im Depot als verkauft, damit schließe ich hier meine letzte Position
                    # => alle Einnahmen müssten verbucht sein
                    if einnahmen != 0:
                        einnahmen = 0

                    if restbuchwert == 0:
                        restbuchwert = amount_in_depot_entry_original - amount_in_depot_entry
                    else:
                        restbuchwert = restbuchwert - amount_in_depot_entry

                    logging.info(f'Nach der Durchführung der Berechnung, habe ich noch die folgende Operation: '
                                 f'{direction} mit der Menge von {stocks_to_sell} Aktien. Bei dem aktuellen Eintrag '
                                 f'habe ich noch {stocks_in_depot_entry} im Depot. Der Buchwert beträgt hierbei '
                                 f'{restbuchwert} und die noch offenen Einnahmen belaufen sich noch auf {einnahmen}')

                    if stocks_in_depot_entry == 0.0:
                        self.close_open_position(stock_in_depot_id)

                else:
                    logging.error("Long Position konnte nicht geschlossen werden!")

                # Update the open position entries
                # If I reduced the amount of stocks I have in the depot, I will adjust my open positions
                if stocks_in_depot_entry_original != stocks_in_depot_entry:
                    self.fifo_positions.loc[self.fifo_positions['transactionID'] == int(
                        row["transactionID"]), 'tradeQuantity'] = stocks_in_depot_entry

                    stock_adjustment = 0.0

                # Update the open position entries
                # If I reduced the amount of stocks, I need to reevaluate my open positions and adjust
                if amount_in_depot_entry_original != restbuchwert:
                    self.fifo_positions.loc[self.fifo_positions['transactionID'] == int(
                        row["transactionID"]), 'amount'] = restbuchwert

                    restbuchwert = 0.0

                # Calculate P&L
                identifier, result = self.calculate_p_l(direction, amount_in_depot_entry, amount_to_sell)
                logging.debug(f'Mit diesem Trade habe ich einen {identifier} gemacht und {result} realisiert!')

                if row["assetCategory"] == "STK" and direction == "SELL":

                    if identifier == "p":
                        self.book_statement(row=row, id="ATG_0000006_0000001",
                                            desc="Aktienverkauf", sdesc="Erlösbuchung",
                                            amount=amount_to_sell, soll=bank_account_id, haben=4852,
                                            account_id=account_id, quality_check_relevant=True)

                        self.book_statement(row=row, id="ATG_0000006_0000002",
                                            desc="Aktienverkauf", sdesc="Abgang des Wertpapiers",
                                            amount=amount_in_depot_entry, soll=4858, haben=1510,
                                            account_id=account_id, quality_check_relevant=False)

                    if identifier == "l":
                        self.book_statement(row=row, id="ATG_0000006_0000003",
                                            desc="Aktienverkauf", sdesc="Aufwandsbuchung",
                                            amount=amount_to_sell, soll=bank_account_id, haben=6892,
                                            account_id=account_id, quality_check_relevant=True)

                        self.book_statement(row=row, id="ATG_0000006_0000004",
                                            desc="Aktienverkauf", sdesc="Abgang des Wertpapiers",
                                            amount=amount_in_depot_entry, soll=6898, haben=1510, account_id=account_id,
                                            quality_check_relevant=False)

                    if identifier == "even":
                        self.book_statement(row=row, id="ATG_0000006_0000020",
                                            desc="Aktienverkauf", sdesc="Schließen eines Long ohne Gewinn oder Verlust",
                                            amount=amount_in_depot_entry, soll=bank_account_id, haben=1510,
                                            account_id=account_id,
                                            quality_check_relevant=True)

                if row["assetCategory"] == "STK" and direction == "BUY":
                    if identifier == "p":
                        self.book_statement(row=row, id="ATG_0000005_0000002 ",
                                            desc="Aktienkauf", sdesc="Gewinnbuchung",
                                            amount=amount_to_sell, soll=4852, haben=bank_account_id,
                                            account_id=account_id, quality_check_relevant=True)

                        self.book_statement(row=row, id="ATG_0000005_0000003",
                                            desc="Aktienkauf", sdesc="Abgang des Wertpapiers",
                                            amount=amount_in_depot_entry, soll=1510, haben=4858, account_id=account_id,
                                            quality_check_relevant=False)

                    if identifier == "l":
                        self.book_statement(row=row, id="ATG_0000005_0000004",
                                            desc="Aktienverkauf", sdesc="Verlustbuchung",
                                            amount=amount_to_sell, soll=6892, haben=bank_account_id,
                                            account_id=account_id, quality_check_relevant=True)

                        self.book_statement(row=row, id="ATG_0000005_0000005",
                                            desc="Aktienverkauf", sdesc="Abgang des Wertpapiers",
                                            amount=amount_in_depot_entry, soll=1510, haben=6898, account_id=account_id,
                                            quality_check_relevant=False)

                if row["assetCategory"] == "STK" and direction == "BUYTOCLOSESHORT":
                    if identifier == "p":
                        self.book_statement(row=row, id="ATG_0000005_0000002 ",
                                            desc="Aktienkauf", sdesc="Gewinnbuchung",
                                            amount=amount_to_sell, soll=4852, haben=bank_account_id,
                                            account_id=account_id, quality_check_relevant=True)

                        self.book_statement(row=row, id="ATG_0000005_0000003",
                                            desc="Aktienkauf", sdesc="Abgang des Wertpapiers",
                                            amount=amount_in_depot_entry, soll=1510, haben=4858, account_id=account_id,
                                            quality_check_relevant=False)

                    if identifier == "l":
                        self.book_statement(row=row, id="ATG_0000005_0000012",
                                            desc="Aktienverkauf", sdesc="Verlustbuchung",
                                            amount=amount_to_sell, soll=6892, haben=bank_account_id,
                                            account_id=account_id, quality_check_relevant=True)

                        self.book_statement(row=row, id="ATG_0000005_0000011",
                                            desc="Aktienverkauf", sdesc="Abgang des Wertpapiers",
                                            amount=amount_in_depot_entry, soll=1510, haben=6898, account_id=account_id,
                                            quality_check_relevant=False)

                    if identifier == "even":
                        self.book_statement(row=row, id="ATG_0000005_0000010",
                                            desc="Aktienverkauf",
                                            sdesc="Schließen eines Shorts ohne Gewinn oder Verlust",
                                            amount=amount_in_depot_entry, soll=1510, haben=1810, account_id=account_id,
                                            # changes test cycle 19.7
                                            quality_check_relevant=True)

                if (row["assetCategory"] == "OPT" or row["assetCategory"] == "FOP") and direction == "BUYTOCLOSESHORT":
                    if identifier == "p":
                        self.book_statement(row=row, id="ATG_0000001_0000002",
                                            desc="Schließen einer verkauften Optionsposition",
                                            sdesc="Ausbuchen der Verbindlichkeit",
                                            amount=amount_to_sell, soll=3500, haben=bank_account_id,
                                            account_id=account_id, quality_check_relevant=True)
                        self.book_statement(row=row, id="ATG_0000001_0000003",
                                            desc="Schließen einer verkauften Optionsposition",
                                            sdesc="Verbuchen des Gewinns",
                                            amount=result, soll=3500, haben=4830, account_id=account_id,
                                            quality_check_relevant=False)

                    if identifier == "l":
                        self.book_statement(row=row, id="ATG_0000001_0000002",
                                            desc="Schließen einer verkauften Optionsposition",
                                            sdesc="Ausbuchen der Verbindlichkeit",
                                            amount=amount_to_sell, soll=3500, haben=bank_account_id,
                                            account_id=account_id, quality_check_relevant=True)
                        self.book_statement(row=row, id="ATG_0000001_0000003",
                                            desc="Schließen einer verkauften Optionsposition",
                                            sdesc="Verbuchen des Verlusts",
                                            amount=result, soll=6300, haben=3500, account_id=account_id,
                                            quality_check_relevant=False)

                    if identifier == "even":
                        self.book_statement(row=row, id="ATG_0000001_0000001",
                                            desc="Schließen einer verkauften Optionsposition",
                                            sdesc="Rückbuchen ohne Gewinn oder Verlust",
                                            amount=amount_to_sell, soll=3500, haben=bank_account_id,
                                            account_id=account_id, quality_check_relevant=True)

            if (row["assetCategory"] == "OPT" or row["assetCategory"] == "FOP") and direction == "SELLTOCLOSELONG":
                if identifier == "p":
                    self.book_statement(row=row, id="ATG_0000002_0000005",
                                        desc="Schließen einer gekauften Optionsposition",
                                        sdesc="Ausbuchen des Ausübungsrechts",
                                        amount=amount_to_sell, soll=bank_account_id, haben=1510,
                                        account_id=account_id, quality_check_relevant=True)
                    self.book_statement(row=row, id="ATG_0000002_0000005",
                                        desc="Schließen einer verkauften Optionsposition",
                                        sdesc="Verbuchen des Gewinns",
                                        amount=result, soll=1510, haben=4830, account_id=account_id,
                                        quality_check_relevant=False)

                if identifier == "l":
                    self.book_statement(row=row, id="ATG_0000002_0000006",
                                        desc="Schließen einer gekauften Optionsposition",
                                        sdesc="Ausbuchen des Ausübungsrechts",
                                        amount=amount_to_sell, soll=bank_account_id, haben=1510,
                                        account_id=account_id, quality_check_relevant=True)
                    self.book_statement(row=row, id="ATG_0000002_0000006",
                                        desc="Schließen einer verkauften Optionsposition",
                                        sdesc="Verbuchen des Verlusts",
                                        amount=result, soll=6300, haben=1510, account_id=account_id,
                                        quality_check_relevant=False)

                if identifier == "even":
                    self.book_statement(row=row, id="ATG_0000002_0000004",
                                        desc="Schließen einer gekauften Optionsposition",
                                        sdesc="Rückbuchen ohne Gewinn oder Verlust",
                                        amount=amount_to_sell, soll=bank_account_id, haben=1510,
                                        account_id=account_id, quality_check_relevant=True)

            if (row["assetCategory"] == "OPT" or row["assetCategory"] == "FOP") and direction == "SELLTOCLOSESHORT":
                if identifier == "p":
                    self.book_statement(row=row, id="ATG_0000001_0000002",
                                        desc="Schließen einer gekauften Optionsposition",  # TODO-Check
                                        sdesc="Ausbuchen der Verbindlichkeit",
                                        amount=amount_to_sell, soll=3500, haben=bank_account_id,
                                        account_id=account_id, quality_check_relevant=True)
                    self.book_statement(row=row, id="ATG_0000001_0000003",
                                        desc="Schließen einer gekauften Optionsposition",
                                        sdesc="Verbuchen des Gewinns",
                                        amount=result, soll=3500, haben=4830, account_id=account_id,
                                            quality_check_relevant=False)

                    if identifier == "l":
                        self.book_statement(row=row, id="ATG_0000001_0000002",
                                            desc="Schließen einer gekauften Optionsposition",  # TODO-Check
                                            sdesc="Ausbuchen der Verbindlichkeit",
                                            amount=amount_to_sell, soll=3500, haben=bank_account_id,
                                            account_id=account_id, quality_check_relevant=True)
                        self.book_statement(row=row, id="ATG_0000001_0000003",
                                            desc="Schließen einer gekauften Optionsposition",
                                            sdesc="Verbuchen des Verlusts",
                                            amount=result, soll=7210, haben=bank_account_id, account_id=account_id,
                                            quality_check_relevant=False)

        return stock_adjustment, restbuchwert, einnahmen

    def account_closure(self, working_dict, soll_account, haben_account, target_soll, target_haben,
                        diff_soll_haben=None):
        ''' Diese Methode berechnet die Different zwischen Soll und Haben und verbucht diese auf das
        entsprechende Gegenkonto'''

        if diff_soll_haben is None:
            diff_soll_haben = working_dict[soll_account] - working_dict[haben_account]

        # Verrechnung auf die Soll und Haben Seite des G+V Kontos
        if diff_soll_haben > 0:
            working_dict[target_soll] = working_dict[target_soll] + abs(diff_soll_haben)
            working_dict[haben_account] = working_dict[haben_account] + abs(diff_soll_haben)
        elif diff_soll_haben < 0:
            working_dict[soll_account] = working_dict[soll_account] + abs(diff_soll_haben)
            working_dict[target_haben] = working_dict[target_haben] + abs(diff_soll_haben)
        elif diff_soll_haben == 0:
            pass

        return working_dict

    def accounting_check(self, journal):
        ''' Diese Methode simuliert den Jahresabschluss um hier noch einmal einen Check zu machen ob die
        Buchunssätze passen, alles verbucht wurde und die Konten am Ende dann abgestimmt sind'''

        accounting = journal.copy()

        # Anlegen der einzelnen Soll und Haben Spalten für die hinterlegten Konten
        account_list = pd.unique(accounting[["SOLL", "HABEN"]].values.ravel("K"))
        account_list = [int(acc) for acc in account_list]

        for acc in account_list:
            accounting[str(acc) + "_S"] = 0.00
            accounting[str(acc) + "_H"] = 0.00

        # Allokation der einzelnen Buchungen auf die Konten (wird ebenfalls abgelegt um die Nachvollziehbarkeit zu haben)
        for index, row in journal.iterrows():
            accounting.loc[index, str(int(row["SOLL"])) + "_S"] = float(row["AMOUNT"])
            accounting.loc[index, str(int(row["HABEN"])) + "_H"] = float(row["AMOUNT"])

        # Summierung der einzelnen Werte über die Konten
        account_summary = {}

        for acc in account_list:
            soll = str(acc) + "_S"
            haben = str(acc) + "_H"
            account_summary[soll] = accounting[soll].sum()
            account_summary[haben] = accounting[haben].sum()

        # Simulation der Jahresabschlusstätigkeiten und Verbuchungen
        year_end_summary = account_summary.copy()
        year_end_summary["Guv_S"] = 0.00
        year_end_summary["Guv_H"] = 0.00
        year_end_summary["EK_S"] = 0.00
        year_end_summary["EK_H"] = 0.00
        year_end_summary["SBK_S"] = 0.00
        year_end_summary["SBK_H"] = 0.00
        bestandskonten = [0, 1, 2, 3]  # diese werden über die Schlussbilanz verrechnet
        erfolgskonten = [4, 5, 6, 7]  # diese werden in die G&V verrechnet

        # GuV Berechnung
        for acc in account_list:
            soll = str(acc) + "_S"
            haben = str(acc) + "_H"

            if int(str(acc)[0]) in erfolgskonten:
                year_end_summary = self.account_closure(year_end_summary, soll, haben, "Guv_S", "Guv_H")

        # Berechnung und Verbuchung des GuV
        year_end_summary["GuV_Final"] = year_end_summary["Guv_H"] - year_end_summary["Guv_S"]
        year_end_summary = self.account_closure(year_end_summary, "Guv_S", "Guv_H", "EK_S", "EK_H")

        # SBK Erstellung
        for acc in account_list:
            soll = str(acc) + "_S"
            haben = str(acc) + "_H"

            if int(str(acc)[0]) in bestandskonten:
                year_end_summary = self.account_closure(year_end_summary, soll, haben, "SBK_S", "SBK_H")

        # Abschluss des Eigenkapitalkontos
        year_end_summary = self.account_closure(year_end_summary, "EK_S", "EK_H", "SBK_S", "SBK_H")

        # Berechnung des Schlussbilanzsaldos
        sbk_saldo = round(year_end_summary["SBK_H"] - year_end_summary["SBK_S"], 2)
        year_end_summary["SBK_Saldo"] = sbk_saldo

        if sbk_saldo == 0:
            logging.info("Quality Check: Validation: Soll und Haben der Schlussbilanz sind abgestimmt!")
        else:
            logging.error("Quality Check: Validation: Soll und Haben der Schlussbilanz unterscheiden sich!}!")

        account_summary = pd.DataFrame(data=account_summary, index=[0])
        accounting_simulation_final = pd.DataFrame(data=year_end_summary, index=[0])

        return accounting, account_summary, accounting_simulation_final

    def generate_MSBuchhalter_Import(self, data_to_import, account, path):
        ''' Diese Methode erstellt die Import-Datei für den MS-Buchhalter 3.0'''

        for index, row in data_to_import.iterrows():
            data_to_import.loc[index, "SOLL"] = str(int(row["SOLL"]))
            data_to_import.loc[index, "HABEN"] = str(int(row["HABEN"]))
            data_to_import.loc[index, "AMOUNT"] = str(row["AMOUNT"]).replace(".", ",")

        import_data = pd.DataFrame()
        import_data["Belegdatum"] = pd.to_datetime(data_to_import["DATE"], format="%Y%m%d").dt.strftime("%d.%m.%Y")
        import_data["Buchungsdatum"] = pd.to_datetime(data_to_import["SETTLEDATE"], format="%Y%m%d").dt.strftime(
            "%d.%m.%Y")
        import_data["Belegnummernkreis"] = ""
        import_data["Belegnummer"] = ""
        import_data["Buchungstext"] = data_to_import["TEXT"]
        import_data["Betrag"] = data_to_import["AMOUNT"]
        import_data["Sollkonto"] = data_to_import["SOLL"]
        import_data["Habenkonto"] = data_to_import["HABEN"]
        import_data["Steuerschlüssel"] = "0"
        import_data["Kostenstelle 1"] = ""
        import_data["Kostenstelle 2"] = ""
        import_data["Währung"] = "EUR"

        path_to_store = os.path.join(path, f"AccountingJournal_Import_MSB_{account}.csv")
        import_data.to_csv(path_or_buf=path_to_store,
                           sep=";",
                           index=False,
                           quotechar='"')

    def processing_check(self, processed_ids, expected_ids):

        try:
            s = expected_ids.merge(processed_ids, on='transactionID', how='left')
            s = s.loc[(s["amount"] != s["processedAmount"])]
        except KeyError:
            s = pd.DataFrame()
        return s

    def generate_single_statements(self, data, open):
        ''' In dieser Methode prüfe ich nun die einzelnen Datensätze und erstelle hier,
            basierend auf den einzelnen Buchungsvorschriften und Fällen die
            einzelnen Buchungssätze '''

        # Setzen der open files des accounts
        self.fifo_positions = open

        # Da IB auch Teilverkäufe vornimmt, habe ich hier einen Abgleich eingebaut,
        # der mir ermöglicht über die einzelen Zeilen hinweg die Trades zu verbuchen
        stock_adjustment = 0.0
        restbuchwert = 0.0
        einnahmen = 0.0

        # Da ich jede Zeile verbuchen muss, prüfe ich jede Zeile einzeln
        for i, row in data.iterrows():
            account_id = row["accountId"]
            position_open = False
            bank_account_id = self.account_mapping[account_id]
            logging.debug(row)

            time.sleep(1 / 1000)

            # Ich prüfe zuerst, ob ich eine offene Position im Depot habe, die ich
            # dann nach dem FIFO-Prinzip verarbeiten muss
            if not row["symbol"] == "":
                if not self.fifo_positions.empty:
                    if row["symbol"] in self.fifo_positions["symbol"].values:
                        position_open = True
                        open_in_depot = self.fifo_positions[self.fifo_positions["symbol"] == row["symbol"]]
                    else:

                        # As I have a Case where IB changed the underlying symbol name, I need to do a seperate check
                        # this is covered in the test case SPECIAL_CASE_DELL
                        if row["activityCode"] == "EXP":  # currently only the case for expirations
                            find_addition = row["underlyingSymbol"].rfind("1")  # check if there is a 1 at the end
                            if find_addition > 0:  # if yes
                                cleaned_string = row["underlyingSymbol"][:-1]  # get the correct string
                                correct_entry = row["symbol"].replace(row["underlyingSymbol"], cleaned_string + " ")

                                # now, check again if I have an open position
                                if correct_entry in self.fifo_positions["symbol"].values:
                                    position_open = True
                                    open_in_depot = self.fifo_positions[self.fifo_positions["symbol"] == correct_entry]
                                    row["symbol"] = correct_entry
                                else:
                                    self.fifo_positions = pd.concat([self.fifo_positions, pd.DataFrame([row])])
                                    position_open = False
                        else:
                            self.fifo_positions = pd.concat([self.fifo_positions, pd.DataFrame([row])])
                            position_open = False
            else:
                if self.fifo_positions.isin([row["activityDescription"]]).any().any():
                    position_open = True
                    open_in_depot = self.fifo_positions[
                        self.fifo_positions["activityDescription"] == row["activityDescription"]]
                else:
                    self.fifo_positions = pd.concat([self.fifo_positions, pd.DataFrame([row])])
                    position_open = False

            logging.debug(f"Das Ergebnis der Suche nach einem offenen Posten ist: {position_open}")

            # Hier drösle ich nun die einzelnen Geschäftsvorfälle auf und
            # berwete diese ja nach Fall und Gegebenheit
            if row["activityCode"] == "ADJ":
                if row["assetCategory"] == "FUT":
                    if row["amount"] < 0:
                        self.book_statement(row=row, id="tbd",
                                            desc="Futures-Handel", sdesc="Verlust",
                                            amount=row["amount"], soll=6300, haben=bank_account_id,
                                            account_id=account_id, quality_check_relevant=True)
                    elif row["amount"] > 0:
                        self.book_statement(row=row, id="tbd",
                                            desc="Futures-Handel", sdesc="Gewinn",
                                            amount=row["amount"], soll=bank_account_id, haben=4905,
                                            account_id=account_id, quality_check_relevant=True)

                    else:  # es kommt vor, dass ich auch 0 € Buchungen habe, da die Kosten für den Verkauf
                        # auf einer Position agregiert werden, d.h. die Teilverkäufe bekommen hier keinen Abzug
                        # daher tracke ich in einem solchen Fall nur dass ich den Datensatz bearbeitet habe,
                        # aber ich buche keine 0-Buchung, da Kostentechnisch nicht relevant
                        self.track_processing(account_id, int(row["transactionID"]), row["amount"], row["date"])

                else:
                    logging.error(f"The following assetCategory is not defined: {row['assetCategory']}")
                    logging.error(f"Der folgende Eintrag aus der Zeile {i} wurde nicht verarbeitet: {row}")
            elif row["activityCode"] == "ASSIGN":

                # ATG_0000004: Zuteilung einer verkauften Optionsposition
                if (row["assetCategory"] == "OPT" or row["assetCategory"] == "FOP") and position_open:

                    # ATG_0000004: Verbuchung der Put-Prämie
                    if row["putCall"] == "P":
                        self.book_statement(row=row, id="ATG_0000004_0000002",
                                            desc="Zuteilung einer verkauften Optionsposition",
                                            sdesc="", amount=abs(open_in_depot["amount"].iloc[0]), soll=3500,
                                            haben=4830, account_id=account_id, quality_check_relevant=False)

                        self.close_open_position(open_in_depot["transactionID"].iloc[0])

                    # ATG_0000004: Verbuchung der Call-Prämie
                    if row["putCall"] == "C":
                        self.book_statement(row=row, id="ATG_0000004_0000003",
                                            desc="Zuteilung einer verkauften Optionsposition",
                                            sdesc="", amount=abs(open_in_depot["amount"].iloc[0]), soll=3500,
                                            haben=4830, account_id=account_id, quality_check_relevant=False)

                        self.close_open_position(open_in_depot["transactionID"].iloc[0])

                # ATG_0000004: Zuteilung einer verkauften Optionsposition
                # Hier muss ich nun die Aktien verbuchen
                elif row["assetCategory"] == "STK":

                    # Kauf von Aktien (Zuteilung des Puts)
                    if row["buySell"] == "BUY":
                        self.book_statement(row=row, id="ATG_0000004_0000001",
                                            desc="Einbuchen einer verkauften Option",
                                            sdesc="", amount=row["amount"], soll=1510, haben=bank_account_id,
                                            account_id=account_id, quality_check_relevant=True)

                        self.add_open_position(row)

                    # Verkauf von Aktien (Ausübung des Calls)
                    if row["buySell"] == "SELL":
                        if position_open == False:  # Falls es keine offenen Positionen gibt, eröffne ich einen Short
                            self.book_statement(row=row, id="ATG_0000004_0000004",
                                                desc="Zuteilung einer verkauften Optionsposition",
                                                sdesc="keine offene Position => Short",
                                                amount=row["amount"], soll=1510, haben=bank_account_id,
                                                account_id=account_id,
                                                quality_check_relevant=True)

                        elif position_open == True:  # Falls offene Position, Short oder Long im Depot?

                            # Wenn ich schon short bin, muss ich die Position erhöhen
                            if open_in_depot["tradeQuantity"].sum() < 0:
                                self.book_statement(row=row, id="ATG_0000004_0000004",
                                                    desc="Zuteilung einer verkauften Optionsposition",
                                                    sdesc="Erhöhung der Shortposition",
                                                    amount=row["amount"], soll=1510, haben=bank_account_id,
                                                    account_id=account_id, quality_check_relevant=True)

                                self.add_open_position(row)  # Eintrag in den offenen Posten

                            # Wenn ich eine bestehende Long-Position habe, muss ich diese FIFO mäßig verarbeiten
                            elif open_in_depot["tradeQuantity"].sum() > 0:
                                stock_adjustment, restbuchwert, einnahmen = self.close_position_fifo(
                                    "SELL", row, open_in_depot, stock_adjustment, restbuchwert, einnahmen,
                                    bank_account_id, account_id=account_id)

                else:
                    logging.error(f"The following assetCategory is not defined: {row['assetCategory']}")
                    logging.error(f"Der folgende Eintrag aus der Zeile {i} wurde nicht verarbeitet: {row}")

                    if row["amount"] == 0:
                        self.track_processing(account_id, row["transactionID"], row["amount"], row["date"])

            elif row["activityCode"] == "BFEE":

                self.book_statement(row=row, id="ATG_0000010_0000001",
                                    desc="Investitionszinsen ", sdesc="Zinszahlung",
                                    amount=row["amount"], soll=7300, haben=bank_account_id, account_id=account_id,
                                    quality_check_relevant=True)

            elif row["activityCode"] == "BUY":

                # Kauf von Aktien
                if row["assetCategory"] == "STK":
                    if position_open == False:  # Falls es keine offenen Positionen gibt, kaufe ich die Aktien
                        self.book_statement(row=row, id="ATG_0000005_0000001",
                                            desc="Aktienkauf", sdesc="keine offene Position",
                                            amount=row["amount"], soll=1510, haben=bank_account_id,
                                            account_id=account_id, quality_check_relevant=True)
                        self.add_open_position(row)

                    elif position_open == True:  # Falls offene Position, Short oder Long im Depot?

                        # Wenn ich schon long bin, muss ich die Position erhöhen
                        if open_in_depot["tradeQuantity"].sum() >= 0:
                            self.book_statement(row=row, id="ATG_0000005_0000001",
                                                desc="Aktienkauf", sdesc="Erhöhung der offenen Position",
                                                amount=row["amount"], soll=1510, haben=bank_account_id,
                                                account_id=account_id, quality_check_relevant=True)

                            # hier muss ich die offene Position manuell hinterlegen
                            self.add_open_position(row)

                        # Wenn ich eine bestehende Short-Position habe, muss ich diese FIFO mäßig verarbeiten
                        elif open_in_depot["tradeQuantity"].sum() < 0:
                            stock_adjustment, restbuchwert, einnahmen = self.close_position_fifo(
                                "BUYTOCLOSESHORT", row, open_in_depot, stock_adjustment, restbuchwert, einnahmen,
                                bank_account_id, account_id)

                # Kauf einer Option
                elif (row["assetCategory"] == "OPT" or row["assetCategory"] == "FOP"):

                    # Fall 1: No open position => create long entry
                    if position_open == False:
                        self.book_statement(row=row, id="ATG_0000002_0000003",
                                            desc="Optionkauf", sdesc="keine offene Position",
                                            amount=row["amount"], soll=1510, haben=bank_account_id,
                                            account_id=account_id, quality_check_relevant=True)
                        # hier muss ich die offene Position manuell hinterlegen
                        self.add_open_position(row)

                    if position_open == True:
                        if open_in_depot["tradeQuantity"].sum() <= 0:
                            stock_adjustment, restbuchwert, einnahmen = self.close_position_fifo(
                                "BUYTOCLOSESHORT", row, open_in_depot, stock_adjustment, restbuchwert, einnahmen,
                                bank_account_id,
                                account_id=account_id)
                        elif open_in_depot["tradeQuantity"].sum() > 0:
                            self.book_statement(row=row, id="Tbd",
                                                desc="Optionkauf", sdesc="Erhöhung der bestehenden Long-Position",
                                                amount=row["amount"], soll=1300, haben=bank_account_id,
                                                account_id=account_id, quality_check_relevant=True)
                            # hier muss ich die offene Position manuell hinterlegen
                            self.add_open_position(row)

                # Gewinn- und Verlustberechnung CFD
                elif row["assetCategory"] == "CFD":

                    if row["amount"] < 0:
                        self.book_statement(row=row, id="ATG_0000010_0000002",
                                            desc="CFD-Handel", sdesc="Verlust",
                                            amount=row["amount"], soll=6300, haben=bank_account_id,
                                            account_id=account_id, quality_check_relevant=True)
                    elif row["amount"] > 0:
                        self.book_statement(row=row, id="ATG_0000010_0000001",
                                            desc="CFD-Handel", sdesc="Gewinn",
                                            amount=row["amount"], soll=bank_account_id, haben=4905,
                                            account_id=account_id, quality_check_relevant=True)
                    else:  # es kommt vor, dass ich auch 0 € Buchungen habe, da die Kosten für den Verkauf
                        # auf einer Position agregiert werden, d.h. die Teilverkäufe bekommen hier keinen Abzug
                        # daher tracke ich in einem solchen Fall nur dass ich den Datensatz bearbeitet habe,
                        # aber ich buche keine 0-Buchung, da Kostentechnisch nicht relevant
                        self.track_processing(account_id, int(row["transactionID"]), row["amount"], row["date"])

                # Gewinn- und Verlustberechnung FUT
                elif row["assetCategory"] == "FUT":

                    if row["amount"] < 0:
                        self.book_statement(row=row, id="tbd",
                                            desc="Future-Handel", sdesc="Verlust",
                                            amount=row["amount"], soll=6300, haben=bank_account_id,
                                            account_id=account_id, quality_check_relevant=True)
                    elif row["amount"] > 0:
                        self.book_statement(row=row, id="tbd",
                                            desc="Future-Handel", sdesc="Gewinn",
                                            amount=row["amount"], soll=bank_account_id, haben=4905,
                                            account_id=account_id, quality_check_relevant=True)
                    else:  # es kommt vor, dass ich auch 0 € Buchungen habe, da die Kosten für den Verkauf
                        # auf einer Position agregiert werden, d.h. die Teilverkäufe bekommen hier keinen Abzug
                        # daher tracke ich in einem solchen Fall nur dass ich den Datensatz bearbeitet habe,
                        # aber ich buche keine 0-Buchung, da Kostentechnisch nicht relevant
                        self.track_processing(account_id, int(row["transactionID"]), row["amount"], row["date"])

                else:
                    logging.error(f"The following assetCategory is not defined: {row['assetCategory']}")
                    logging.error(f"Der folgende Eintrag aus der Zeile {i} wurde nicht verarbeitet: {row}")

            elif row["activityCode"] == "CFD":  # CFD Interest and Fees
                # Unter diesem Code werden nur die Kursdifferenzen und Zinsen des CFD Handels aufgeführt,
                # die Käufe- und Verkäufe werden wie bei den Aktien unter Sell und Buy getätigt

                # Zinsen
                if (row["symbol"] == "") and "CFD INTEREST" in row["activityDescription"]:

                    if row["amount"] <= 0:
                        self.book_statement(row=row, id="ATG_0000009_0000001",
                                            desc="CFD-Handel", sdesc="Zinsaufwendung",
                                            amount=row["amount"], soll=7300, haben=bank_account_id,
                                            account_id=account_id,
                                            quality_check_relevant=True, text=row["activityDescription"])
                        self.close_open_position(row["transactionID"])

                    elif row["amount"] > 0:
                        self.book_statement(row=row, id="ATG_0000009_0000002",
                                            desc="CFD-Handel", sdesc="Zinsgewinne",
                                            amount=row["amount"], soll=bank_account_id, haben=7300,
                                            account_id=account_id,
                                            quality_check_relevant=True, text=row["activityDescription"])
                        self.close_open_position(row["transactionID"])

                # Kursdifferenzen
                if (row["symbol"] != "") and "USD" in row["activityDescription"]:

                    # Kursverluste
                    if row["amount"] < 0:
                        self.book_statement(row=row, id="ATG_0000010_0000004",
                                            desc="CFD-Handel", sdesc="Kursverlust",
                                            amount=row["amount"], soll=6880, haben=bank_account_id,
                                            account_id=account_id, quality_check_relevant=True,
                                            text=row["activityDescription"])
                    # Kursgewinne
                    elif row["amount"] > 0:
                        self.book_statement(row=row, id="ATG_0000010_0000003",
                                            desc="CFD-Handel", sdesc="Kursgewinn",
                                            amount=row["amount"], soll=bank_account_id, haben=4840,
                                            account_id=account_id, quality_check_relevant=True,
                                            text=row["activityDescription"])
                    else:
                        logging.error("Der Trade konnte nicht verbucht werden!")

            elif row["activityCode"] == "CINT":  # Credit Interest on cash balances

                if row["amount"] <= 0:
                    self.book_statement(row=row, id="ATG_0000009_0000001",
                                        desc="Zinsaufwendungen", sdesc="Bezahlte Zinsen",
                                        amount=row["amount"], soll=7300, haben=bank_account_id, account_id=account_id,
                                        quality_check_relevant=True, text=row["activityDescription"])
                elif row["amount"] > 0:
                    self.book_statement(row=row, id="ATG_0000009_0000002",
                                        desc="Zinsaufwendungen", sdesc="Erhaltene Zinsen",
                                        amount=row["amount"], soll=bank_account_id, haben=7100, account_id=account_id,
                                        quality_check_relevant=True, text=row["activityDescription"])

            elif row["activityCode"] == "DINT":
                if row["amount"] <= 0:
                    self.book_statement(row=row, id="ATG_0000009_0000001",
                                        desc="Zinsaufwendungen", sdesc="Bezahlte Zinsen",
                                        amount=row["amount"], soll=7300, haben=bank_account_id, account_id=account_id,
                                        quality_check_relevant=True, text=row["activityDescription"])
                elif row["amount"] > 0:
                    self.book_statement(row=row, id="ATG_0000009_0000002",
                                        desc="Zinsaufwendungen", sdesc="Erhaltene Zinsen",
                                        amount=row["amount"], soll=bank_account_id, haben=7100, account_id=account_id,
                                        quality_check_relevant=True, text=row["activityDescription"])

            elif row["activityCode"] == "DIV":
                self.book_statement(row=row, id="ATG_0000008_0000001",
                                    desc="Dividendeneinnahmen", sdesc="Verbuchung der Dividenden",
                                    amount=row["amount"], soll=bank_account_id, haben=7020, account_id=account_id,
                                    quality_check_relevant=True)

            elif row["activityCode"] == "EXP":
                # ATG_0000003: Expiration einer Stillhalterposition
                if (row["assetCategory"] == "OPT" or row["assetCategory"] == "FOP"):
                    if position_open:
                        self.book_statement(row=row, id="ATG_0000003_0000001",
                                            desc="Expiration einer Stillhalterposition", sdesc="Verbuchen des Gewinns",
                                            amount=abs(open_in_depot["amount"].iloc[0]), soll=3500, haben=4830,
                                            account_id=account_id, quality_check_relevant=False)

                        self.close_open_position(open_in_depot["transactionID"].iloc[0])

                    else:
                        if row["amount"] == 0:
                            self.track_processing(account_id, row["transactionID"], row["amount"], row["date"])

                else:
                    logging.error(f"The following assetCategory is not defined: {row['assetCategory']}")
                    logging.error(f"Der folgende Eintrag aus der Zeile {i} wurde nicht verarbeitet: {row}")

            elif row["activityCode"] == "FOREX":
                if row["assetCategory"] == "CASH":
                    if row["amount"] > 0:
                        self.book_statement(row=row, id="tbd",
                                            # TODO: neuer Fall, Konto muss noch geprüft werden ob richtig
                                            desc="Währungsumrechnung", sdesc="Verbuchung des Gewinns",
                                            amount=row["amount"], soll=bank_account_id, haben=4840,
                                            account_id=account_id,
                                            quality_check_relevant=True)
                    elif row["amount"] < 0:
                        self.book_statement(row=row, id="tbd",
                                            # TODO: neuer Fall, Konto muss noch geprüft werden ob richtig
                                            desc="Währungsumrechnung", sdesc="Verbuchung des Verlusts",
                                            amount=row["amount"], soll=bank_account_id, haben=6880,
                                            account_id=account_id,
                                            quality_check_relevant=True)

                    elif row["amount"] == 0:
                        pass

                    else:
                        logging.error(f"The following assetCategory is not defined: {row['assetCategory']}")
                        logging.error(f"Der folgende Eintrag aus der Zeile {i} wurde nicht verarbeitet: {row}")

            elif row["activityCode"] == "FRTAX":
                self.book_statement(row=row, id="ATG_0000008_0000002",
                                    desc="Dividendeneinnahmen", sdesc="Verbuchung der Quellsteuer",
                                    amount=row["amount"], soll=7639, haben=bank_account_id, account_id=account_id,
                                    quality_check_relevant=True)

            elif row["activityCode"] == "OFEE":

                if row["amount"] <= 0:
                    self.book_statement(row=row, id="ATG_0000007_0000001",
                                        desc="Marktdatengebuehren", sdesc="Verbuchung der Kosten",
                                        amount=row["amount"], soll=6300, haben=bank_account_id, account_id=account_id,
                                        quality_check_relevant=True)

                elif row["amount"] > 0:
                    self.book_statement(row=row, id="ATG_0000007_0000002",
                                        desc="Marktdatengebuehren", sdesc="Verbuchung der Gutschrift",
                                        amount=row["amount"], soll=bank_account_id, haben=6300, account_id=account_id,
                                        quality_check_relevant=True)

            elif row["activityCode"] == "PIL":
                self.book_statement(row=row, id="",
                                    # TODO => neu, Ersatzzahlung Dividenden https://ibkr.info/article/2713
                                    desc="Dividendeneinnahmen", sdesc="Payment in Lieu of Dividend (Ordinary Dividend)",
                                    amount=row["amount"], soll=bank_account_id, haben=7020, account_id=account_id,
                                    quality_check_relevant=True)

            elif row["activityCode"] == "SELL":

                if row["assetCategory"] == "CFD":
                    # Bei den CFD's habe ich keine direkte Position, sondern mir werden täglich die Gebühren
                    # in Rechnung gestellt, somit ist es hier egal, ob eine Position eröffnet ist oder nicht
                    # wenn keine Position eröffnet ist, gehe ich davon aus, dass diese noch besteht und ich die
                    # verrechnung zu Recht bekomme. Daher schließe ich auch hier keine Position
                    if row["amount"] < 0:
                        self.book_statement(row=row, id="ATG_0000010_0000002",
                                            desc="CFD-Handel", sdesc="Verlust",
                                            amount=row["amount"], soll=6300, haben=bank_account_id,
                                            account_id=account_id, quality_check_relevant=True)
                    elif row["amount"] > 0:
                        self.book_statement(row=row, id="ATG_0000010_0000001",
                                            desc="CFD-Handel", sdesc="Gewinn",
                                            amount=row["amount"], soll=bank_account_id, haben=4905,
                                            account_id=account_id, quality_check_relevant=True)

                        self.close_open_position(int(row["transactionID"]))
                    else:  # es kommt vor, dass ich auch 0 € Buchungen habe, da die Kosten für den Verkauf
                        # auf einer Position agregiert werden, d.h. die Teilverkäufe bekommen hier keinen Abzug
                        # daher tracke ich in einem solchen Fall nur dass ich den Datensatz bearbeitet habe,
                        # aber ich buche keine 0-Buchung, da Kostentechnisch nicht relevant
                        self.track_processing(account_id, int(row["transactionID"]), row["amount"], row["date"])

                elif row["assetCategory"] == "FUT":
                    # Bei den Futures habe ich keine direkte Position, sondern mir werden täglich die Gebühren
                    # in Rechnung gestellt, somit ist es hier egal, ob eine Position eröffnet ist oder nicht
                    # wenn keine Position eröffnet ist, gehe ich davon aus, dass diese noch besteht und ich die
                    # verrechnung zu Recht bekomme. Daher schließe ich auch hier keine Position da ich weder eine
                    # offene, noch eine geschlossene Position tracke => falls ich jemald auch den Abgleich auf
                    # direkter Ebene zu meinem Depot machen will, muss ich die Positionan manuell eröffnen und
                    # schließen
                    if row["amount"] < 0:
                        self.book_statement(row=row, id="ATG_0000010_0000020",
                                            desc="Futures-Handel", sdesc="Verlust",
                                            amount=row["amount"], soll=6300, haben=bank_account_id,
                                            account_id=account_id, quality_check_relevant=True)
                    elif row["amount"] > 0:
                        self.book_statement(row=row, id="ATG_0000010_0000025",
                                            desc="Futures-Handel", sdesc="Gewinn",
                                            amount=row["amount"], soll=bank_account_id, haben=4905,
                                            account_id=account_id, quality_check_relevant=True)
                    else:  # es kommt vor, dass ich auch 0 € Buchungen habe, da die Kosten für den Verkauf
                        # auf einer Position agregiert werden, d.h. die Teilverkäufe bekommen hier keinen Abzug
                        # daher tracke ich in einem solchen Fall nur dass ich den Datensatz bearbeitet habe,
                        # aber ich buche keine 0-Buchung, da Kostentechnisch nicht relevant
                        self.track_processing(account_id, int(row["transactionID"]), row["amount"], row["date"])

                elif (row["assetCategory"] == "OPT" or row["assetCategory"] == "FOP"):
                    # If I do not have an open position, I will short a position
                    if position_open == False:
                        self.book_statement(row=row, id="ATG_0000002_0000001",
                                            desc="Eröffnen einer Stillhalterposition",
                                            sdesc="Eröffnung ohne bestehende Long-Position",
                                            amount=row["amount"], soll=bank_account_id, haben=3500,
                                            account_id=account_id, quality_check_relevant=True)
                        self.add_open_position(row)

                    # TODO: Position erhöhen, oder Teilposition schließen
                    elif position_open == True:

                        # Wenn ich schon short bin, muss ich meine Position erhöhen
                        if open_in_depot["tradeQuantity"].sum() < 0:
                            self.book_statement(row=row, id="ATG_0000002_0000002",
                                                desc="Eröffnen einer Stillhalterposition",
                                                sdesc="Erhöhung der Shortposition",
                                                amount=row["amount"], soll=bank_account_id, haben=3500,
                                                account_id=account_id, quality_check_relevant=True)

                            self.add_open_position(row)  # Add entry to open position

                        # If I'm long in the option, I need to close it FIFO
                        elif open_in_depot["tradeQuantity"].sum() > 0:
                            stock_adjustment, restbuchwert, einnahmen = self.close_position_fifo(
                                "SELLTOCLOSELONG", row, open_in_depot, stock_adjustment, restbuchwert, einnahmen,
                                bank_account_id,
                                account_id=account_id)

                elif (row["assetCategory"] == "STK"):
                    if position_open == False:  # Falls es keine offenen Positionen gibt, eröffne ich einen Short
                        self.book_statement(row=row, id="ATG_0000006_0000005",
                                            desc="Aktienverkauf", sdesc="Sell-Short, ohne offene Position",
                                            amount=row["amount"], soll=bank_account_id, haben=1510,
                                            account_id=account_id, quality_check_relevant=True)
                        self.add_open_position(row)

                    elif position_open == True:  # Falls offene Position, Short oder Long im Depot?

                        # Wenn ich schon short bin, muss ich die Position erhöhen
                        if open_in_depot["tradeQuantity"].sum() < 0:
                            self.book_statement(row=row, id="ATG_0000006_0000005",
                                                desc="Aktienverkauf", sdesc="Erhöhung der Shortposition",
                                                amount=row["amount"], soll=bank_account_id, haben=1510,
                                                account_id=account_id, quality_check_relevant=True)

                            self.add_open_position(row)

                        # Wenn ich eine bestehende Long-Position habe, muss ich diese FIFO mäßig verarbeiten
                        # Die Umsetzung ist in dem Prozessfluss ATG_0000006_Aktienverkauf_Close_Long beschrieben
                        elif open_in_depot["tradeQuantity"].sum() > 0:
                            stock_adjustment, restbuchwert, einnahmen = self.close_position_fifo(
                                "SELL", row, open_in_depot, stock_adjustment, restbuchwert, einnahmen, bank_account_id,
                                account_id=account_id)

                else:
                    logging.error(f"The following assetCategory is not defined: {row['assetCategory']}")
                    logging.error(f"Der folgende Eintrag aus der Zeile {i} wurde nicht verarbeitet: {row}")

            elif row["activityCode"] == "STAX":

                self.book_statement(row=row, id="ATG_0000007_0000003",
                                    desc="Marktdatengebuehren", sdesc="Steuerverbuchung",
                                    amount=row["amount"], soll=6300, haben=bank_account_id, account_id=account_id,
                                    quality_check_relevant=True)

            else:
                logging.error(f"The following activity code is not defined: {row['activityCode']}")
                logging.error(f"Der folgende Eintrag aus der Zeile {i} wurde nicht verarbeitet: {row}")

        if self.journal.empty:
            journal = self.journal
        else:
            journal = self.journal[
                ["Account", "Belegnummer", "SATZ_ID", "DESC", "SUBDESC", "DATE", "SETTLEDATE", "TEXT", "AMOUNT", "SOLL",
                 "HABEN", "QUALITYREL"]]

        return data, journal, self.fifo_positions

    def generate_booking_journal(self, accounts_to_combine, types_to_process=None):
        ''' Das ist die Hauptmethode, hier wird der Ablauf gesteuert um das Buchungssjournal zu erstellen'''

        # Einlesen der Daten, die verarbeitet werden sollen, die offenen Positionen werden
        # weiter unten pro Account eingelesen
        imported_data = self.imported_data
        modified_data = imported_data
        results = []

        # Kombinieren der Accounts, falls es eine Migration von IB-Konten gab.
        # Da es eine Migration und Kombination ist, handelt es sich um das selbe Konto und es wird
        # bei der Berechnung auch so weitergeführt, um die FIFO-Daten sauber zu berechnen
        dict_has_entry = bool(accounts_to_combine)

        if dict_has_entry:
            for key, value in accounts_to_combine.items():
                modified_data = modified_data.replace(to_replace=key, value=value)

                for index, item in enumerate(self.accounts):
                    if item == key:
                        self.accounts[index] = value

        self.accounts = list(set(self.accounts))
        logging.debug(f"Die folgenden Accounts werden berücksichtigt: {self.accounts}")

        # Jeder Account wird einzeln betrachtet da für jeden das FIFO Prinzip gesondert gilt!
        for account in self.accounts:
            logging.info(f"The following account will now be processed: {account} .........")

            # Selektion der Accountdaten
            data = modified_data[modified_data["accountId"] == account]
            self.modified_data = data

            # Schritt 01: Löschen der IB-Internen Statuszeilen:
            # - Starting Balance
            # - FX Translation P&L
            # - Ending Balance
            data = data[(data["activityDescription"] != "Starting Balance")]
            data = data[(data["activityDescription"] != "FX Translations P&L")]
            data = data[(data["activityDescription"] != "Ending Balance")]

            # Schritt 02: Löschen der Bankbewegungen
            # Diese müssen manuell gebucht werden um Doppelbuchungen zu vermeiden
            # TODO: hier kann ich noch den Buchhungssatz für den Transfer zwischen den IB-Accounts einbauen
            bank_transfers = data[(data["activityCode"] == "WITH") | (data["activityCode"] == "DEP")]
            data = data[~data.isin(bank_transfers)].dropna()

            # Schritt 03: Sortieren der Buchungen nach der Transaktions-ID um Fehlbuchungen zu vermeiden und
            # filtern der Daten nach dem Datum
            data.sort_values(by='transactionID', ascending=True, inplace=True)
            data = data[data["date"] >= float(self.start)]
            data = data[data["date"] <= float(self.end)]

            ##################################################################################################
            # Debug Hilfen
            # list_to_check = [400141989, ]
            # data = data.loc[data["transactionID"].isin(list_to_check)]

            list_to_check = ["OFEE", ]
            # data = data.loc[data["assetCategory"].isin(list_to_check)]
            data = data.loc[data["activityCode"].isin(list_to_check)]
            # data = data.loc[data["symbol"].isin(["UA",])]

            ##################################################################################################

            # Quality Check - IB Report: Cash Report - Broker Interest Paid and Received
            # list_to_check = ["DINT", "CINT", "BFEE"]
            # data = data.loc[data["activityCode"].isin(list_to_check)]

            # Quality Check - IB Report: Cash Report - Dividends
            # list_to_check = ["DIV",]
            # data = data.loc[data["activityCode"].isin(list_to_check)]

            # Quality Check - IB Report: Cash Report - Withholding Tax
            # list_to_check = ["FRTAX",]
            # data = data.loc[data["activityCode"].isin(list_to_check)]

            # Quality Check - IB Report: Cash Report - Sales Tax
            # list_to_check = ["STAX",]
            # data = data.loc[data["activityCode"].isin(list_to_check)]

            # Quality Check - IB Report: Cash Report - CFD Charges
            # list_to_check = ["CFD",]
            # data = data.loc[data["activityCode"].isin(list_to_check)]

            # Quality Check - IB Report: Cash Report - Other Fees
            # list_to_check = ["OFEE",]
            # data = data.loc[data["activityCode"].isin(list_to_check)]

            # Quality Check - IB Report: Commissions
            print(f"The trade commission in total is {data['tradeCommission'].sum()}")

            ##################################################################################################

            # Schritt 04: Laden der offenen Positionen
            open_filename = os.path.join(self.pickle_files, f"OpenPositions_{account}.pkl")
            open = pd.read_pickle(open_filename)

            # Schritt 05: Erstellen der einzelnen Buchungsdaten => Methode: Generate Single Statements
            data, journal, open = self.generate_single_statements(data, open)

            # Schritt 06: Löschen der einzenen, nicht relevanten Einträge aus der Open-Trade Liste,
            # wie z.B. die Dividendenzahlungen, Margin Variation Zahlungen, etc.
            open = self.delete_selected_fifo_positions(open)

            # Schritt 07: Quality Checks und Fehlerhandling!

            # Schritt 07.01. - Abgleich der Salden aus den einzelnen Datenlisten
            modified_data_amount = round(sum(abs(data["amount"])), 2)

            if journal.empty:
                quality_check = "no bookings generated"
                journal_data_amount = 0
            elif modified_data.empty:
                quality_check = "no bookings generated"
                modified_data_amount = 0
            else:
                # Only take relevant data into consideration, flag is set in the booking statements
                data_check_journal_data = journal[journal["Account"] == account]
                journal_data_amount = round(
                    sum(abs(data_check_journal_data[data_check_journal_data["QUALITYREL"] == True]["AMOUNT"])), 2)

                if (journal_data_amount == modified_data_amount) or (modified_data.empty and journal_data_amount.empty):
                    quality_check = "erfolgreich"
                else:
                    quality_check = "nicht erfolgreich"

            results.append(quality_check)

            # Ausgabe des Ergebnisses
            print(f"Qualitätscheck Validierung: {quality_check}, "
                  f"die Journalsummer ist {journal_data_amount} und "
                  f"die der verabrbeiteten Daten ist {modified_data_amount}, Account {account}")

            # Schritt 07.2. - Prüfung ob alle Zeilen verarbeitet wurden
            # Die Daten, die in den "modified data" waren und nicht in den processed IDs aufgenommen wurden
            # wurden nicht verarbeitet. Optimalerweise sind alle zeilen verarbeitet worden

            try:
                if self.processed_entries:
                    processed_for_this_account = self.processed_entries[self.processed_entries["account"] == account]
                    not_processed = pd.concat(
                        [data["transactionID"], processed_for_this_account["transactionID"]]).drop_duplicates(
                        keep=False)
            except ValueError:
                not_processed = data["transactionID"]

            # Schritt 07.3. - Buchungsdatum berücksichtigen
            # IB gibt nicht bei allen Zeilen ein Settle-Datum aus und daher nehme ich überall wo es ausgegeben wird,
            # das IB Buchungsdatum und wo es nicht ausgegeben wird das Datum was ein IB in dem Datumsfeld angibt

            for index, row in journal.iterrows():
                if row["SETTLEDATE"] == "":
                    journal.loc[index, "SETTLEDATE"] = row["DATE"]

            # Schritt 08:
            # Simulation der Buchhaltung und des Jahresabschlusses
            accounting = pd.DataFrame()
            if not journal.empty:
                simulation_journal = journal[journal["Account"] == account]
                accounting, account_summary, accounting_simulation_final = self.accounting_check(simulation_journal)

            # Schritt 09:
            # Nun speichere ich die ganzen Daten noch in einer Excel, um diese dann final abzulegen
            if save_to_excel:
                # create the writer object
                path_to_store = os.path.join(self.dir_export, f"AccountingJournal_{account}.xlsx")
                writer = pd.ExcelWriter(path_to_store, engine='xlsxwriter')
                path_open_positions = os.path.join(self.dir_export, f"OpenPositions_{account}.xlsx")
                writer_open_positions = pd.ExcelWriter(path_open_positions, engine='xlsxwriter')

                # save the data to the excel

                # Imported or Downloaded Data
                df_to_store = imported_data[imported_data["accountId"] == account]
                df_to_store.to_excel(writer, sheet_name='Downloaded_Data', index=False)

                # From the program modified data
                df_to_store = data[data["accountId"] == account]
                df_to_store.to_excel(writer, sheet_name='Modified_Data', index=False)

                # Excluded bank transfer data
                df_to_store = bank_transfers[bank_transfers["accountId"] == account]
                df_to_store.to_excel(writer, sheet_name='Bank_Transfers', index=False)

                # Booking Journal
                if not journal.empty:
                    df_to_store = journal[journal["Account"] == account]
                    df_to_store.to_excel(writer, sheet_name="Booking_Journal", index=False)

                # Open FIFO Positions
                if not open.empty:
                    df_to_store = open[open["accountId"] == account]
                    df_to_store.to_excel(writer, sheet_name=f"FIFO_Positions", index=False)
                    df_to_store.to_excel(writer_open_positions, sheet_name=f"OpenPositions", index=False)
                    writer_open_positions.save()

                # Processed ID's
                if not self.processed_entries.empty:
                    df_to_store = self.processed_entries[self.processed_entries["account"] == account]
                    df_to_store.to_excel(writer, sheet_name="Processed_ID", index=False)
                    # not_processed.to_excel(writer, sheet_name="Not_Processed", index=False)

                # Accounting Simulation
                if not accounting.empty:
                    accounting.to_excel(writer, sheet_name="Acc_Sim_1", index=False)
                    account_summary.to_excel(writer, sheet_name="Acc_Sim_2", index=False)
                    accounting_simulation_final.to_excel(writer, sheet_name="Acc_Sim_3", index=False)

                writer.save()

            # Schritt 10:
            # Erstellung der Buchungssatz - Importdatei
            if not journal.empty:
                data_to_import = journal[journal["Account"] == account]
                self.generate_MSBuchhalter_Import(data_to_import, account, self.dir_export)

            # Schritt 11:
            # Ausgabe der Informationen zum Abgleich mit den Testdaten
            print(f'Journalsumme: {journal_data_amount}')
            print(f'Verarbeitete Daten: {modified_data_amount}')
            try:
                print(f'Gewinn oder Verlust: {accounting_simulation_final["GuV_Final"][0]}')
            except UnboundLocalError:
                print("Gewinn oder Verlust: no statement calculated")
