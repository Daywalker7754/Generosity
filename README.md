# Generosity
Program to create interactive brokers accounting statements for a German cooperation

Dieses Programm soll die Trades aus Interactive Brokers verarbeiten und gemäß den aktuellen Buchhaltungsregelnd die Buchungssätze für eine doppelte Buchführung erstellen. Es ist dazu gedacht, dass die Buchhaltung mit Hilfe dieses Programms selbst und automatisch gemacht werden kann, um sich Kosten in der GmbH zu sparen.
Aktuell unterstützt dieses Programm:
- Aktien
- Futures
- CFD’s
- Optionen

Das Konzept wird aktuell auf den Kontenrahmen SKR04 angewendet.

Um das Programm zu nutzen, müssen die folgenden Schritte durchgeführt werden:
1.	Download der Interactive Brokers XML-Datei “Statement of Funds” (dt. Kapitalflussbericht) für ein abgeschlossenes Geschäftsjahr
2.	Speichern der IB-Download-Datei in dem Order „import“
3.	Anpassen der configuration.ini
4.	Ausführen des Programms
5.	Prüfen der Ergebnisse
