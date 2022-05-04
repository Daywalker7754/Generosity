import os
import xml.etree.ElementTree as eTree
from contextlib import suppress

from ib_insync import FlexReport
from ib_insync import util
from ib_insync.objects import DynamicObject

from src.PathHandler import PathHandler


class ImportHandler:

    def __init__(self):
        # Zuerst muss ich hier einmal die Pfade erstellen
        self.dir = PathHandler()
        dir = self.dir.get_working_dir()
        self.dir_pickle_file = os.path.join(dir, "working_files", "cleaned_data.pkl")
        self.dir_excel_backup = os.path.join(dir, "working_files", "ib_statement_prepared.xlsx")
        self.dir_import = os.path.join(dir, "import")
        self.data = None
        self.root = None

    def __load__(self, path):
        with open(path, 'rb') as file:
            self.data = file.read()
            self.root = eTree.fromstring(self.data)

    def __extract__(self, topic: str, parseNumbers=True) -> list:
        """
        Extract items of given topic and return as list of objects.
        The topic is a string like TradeConfirm, ChangeInDividendAccrual,
        Order, etc.
        """
        cls = type(topic, (DynamicObject,), {})
        results = [cls(**node.attrib) for node in self.root.iter(topic)]
        if parseNumbers:
            for obj in results:
                d = obj.__dict__
                for k, v in d.items():
                    with suppress(ValueError):
                        d[k] = float(v)
                        d[k] = int(v)
        return results

    def __prepare_dataframe__(self, topic: str, parseNumbers=True):
        """Same as extract but return the result as a pandas DataFrame."""
        return util.df(self.__extract__(topic, parseNumbers))

    def __store_dataframes__(self, data):
        data.to_pickle(self.dir_pickle_file)

    def __clean_StatementOfFundsLine__(self, data):

        cleaned_funds = data[[
            "accountId",
            "transactionID",
            "tradeID",
            "orderID",
            "date",
            "reportDate",
            "settleDate",
            "activityCode",
            "assetCategory",
            "symbol",
            "description",
            "conid",
            "underlyingConid",
            "isin",
            "underlyingSymbol",
            "activityDescription",
            "buySell",
            "putCall",
            "multiplier",
            "strike",
            "expiry",
            "tradeQuantity",
            "tradePrice",
            "tradeGross",
            "tradeCommission",
            "currency",
            "debit",
            "credit",
            "amount",
            "tradeCode",
            "balance",
            "levelOfDetail"
        ]]

        cleaned_funds.to_excel(self.dir_excel_backup)
        self.__store_dataframes__(cleaned_funds)

        print("The statement of funds was prepared successfully....")
        return cleaned_funds

    def get_report_topics(self):
        """Get the set of topics that can be extracted from this report."""
        return set(node.tag for node in self.root.iter() if node.attrib)

    def import_ib_xml_manual(self, import_filename):
        ''' Hier importiere ich den Kapitalflussbericht, den ich manuell von IB heruntergeladen habe'''
        path_import_file = os.path.join(self.dir_import, import_filename)
        self.__load__(path_import_file)
        funds = self.__prepare_dataframe__("StatementOfFundsLine")
        cleaned_data = self.__clean_StatementOfFundsLine__(funds)
        return cleaned_data

    def import_ib_xml_automatic(self, token, queryid):  # TODO, habe ich explzit ausgebaut, muss hier einmal die Dinge
        # anpassen dass ich auch die einzelnen Punkte zu verschiedenen Punkten laden kann

        report = FlexReport()
        report.download(token, queryid)
        funds = report.df("StatementOfFundsLine")
        cleaned_data = self.__clean_StatementOfFundsLine__(funds)

        return cleaned_data
