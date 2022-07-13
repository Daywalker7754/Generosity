import os
from configparser import ConfigParser

from src.PathHandler import PathHandler


class ConfigHandler:

    def __init__(self):
        paths = PathHandler()
        self.dir = paths.get_working_dir()

    def write_config(self):
        config = ConfigParser()

        with open('configuration.ini', 'w') as configfile:
            config.add_section('Import')
            config.set("Import", "Dateiname Kapitalflussbericht", "Kapitalflussbericht.xml")
            config.add_section('Accounts')
            config.set("Accounts", "IB-Accounts", "U7876826, U4876826, U6834633")

            config.add_section('IBAccountMappingToAccounting')
            config.set("IBAccountMappingToAccounting", "U7876826", "1810")
            config.set("IBAccountMappingToAccounting", "U4876826", "1811")
            config.set("IBAccountMappingToAccounting", "U6834633", "1812")

            config.add_section('IBTransferMapping')
            config.set("IBTransferMapping", "U4876826", "U7876826")
            config.write(configfile)

    def read_config(self):
        settings = ConfigParser()
        path = os.path.join(self.dir, "configuration.ini")
        settings.read(path)

        return settings

    def get_statement_of_funds_name(self):

        try:
            print(self.dir)

            settings = self.read_config()
            name = settings["Import"]["Dateiname Kapitalflussbericht"]

        except KeyError:
            print(self.dir)

        return name

    def get_ib_accounts(self):
        settings = self.read_config()
        accounts = settings["Accounts"]["IB-Accounts"]

        if type(accounts) == str:
            accounts = accounts.replace(" ", "")
            accounts = accounts.split(",")

        return accounts

    def get_ib_to_accounting_map(self):
        settings = self.read_config()

        dict = {}

        for acc in self.get_ib_accounts():
            try:
                dict[acc] = int(settings["IBAccountMappingToAccounting"][acc])
            except KeyError:
                print("Error, you did not map all IB accounts to your Accounting")

        return dict

    def get_ib_acc_combination(self):
        settings = self.read_config()

        dict = {}

        for acc in self.get_ib_accounts():
            try:
                dict[acc] = settings["IBTransferMapping"][acc]
            except KeyError:
                pass

        return dict

    # accounts_to_process = ["U7876826", "U4876826", "U6834633"]


if __name__ == '__main__':
    ch = ConfigHandler()
    ch.write_config()
    ch.read_config()
    ch.get_statement_of_funds_name()
    x = ch.get_ib_to_accounting_map()

    x = ch.get_ib_acc_combination()

    print(x)
