# ProgAlg Task 12: Type-Token-Ratio 

Alles was wir zum Bearbeiten der Aufgabe brauchen hab ich hier in das Repo gepackt. Die Texte hab ich erstmal so 
gelassen wie er sie in DropBox hochgeladen hat. Ich habe nur die Leerzeichen und Kommas aus den Dateinamen entfernt. Ob 
und wie wir die dann noch duplizieren um die Arbeitslast zu erhöhen schauen wir später (das ist in der Aufgabenstellung 
empfohlen, nicht explizit gefordert).


## Repo Struktur

**data/stopwords** - Stopword .json Dateien für alle Sprachen<br>
**data/text/<sprache>** - alle bereitgestellten Texte aus Dropbox<br>
**latex/** - Dokumentation im doku.tex File und Bilder für die Doku (der Rest ist kompiliert uns soll lokal bleiben)<br> 
**src/main/java** - Standard Ordner für Java Source Code (Hier sucht Maven nach dem Code für die jars)<br>

## How To Use

Jar erzeugen:<br>
Run `$ mvn package` im Reposiroty Root Folder.

Programm Ausführen:<br>
Run `$ YOUR_SPARK_HOME/bin/spark-submit --class "WordCount" --master "local[4]" path/to/target/prog-alg-1.0.jar path/to/textfile`<br>
Die WordCount Klasse ist erstmal nur zum ausprobieren ob/wie das funktioniert. Wir können jetzt eine eigene Javaklasse 
für die Aufgabe schreiben. Die ist dann nach dem erzeugen der Jar in der selben Jar wie WordCount und muss dann zum  
Ausführen mit dem --class Parameter statt "WordCount" angegeben werden.
