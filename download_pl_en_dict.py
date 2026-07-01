"""
download_pl_en_dict.py
======================
Creates a Polish-English dictionary from a built-in word list.
2000+ pairs covering CDI-relevant vocabulary AND common non-CDI words
so the shared associate set remains large after CDI words are removed.

Saves to data/pl_en_dict.csv
"""

import csv
import os
from collections import defaultdict

DATA_DIR = "data"
DICT_OUT = os.path.join(DATA_DIR, "pl_en_dict.csv")


def build_builtin_dictionary():
    pairs = [
        # ── Travel & Transport ──
        ("passport", "paszport"), ("visa", "wiza"), ("ticket", "bilet"),
        ("luggage", "bagaż"), ("suitcase", "walizka"), ("backpack", "plecak"),
        ("itinerary", "plan podróży"), ("reservation", "rezerwacja"),
        ("boarding pass", "karta pokładowa"), ("departure", "odlot"),
        ("arrival", "przylot"), ("terminal", "terminal"),
        ("customs", "odprawa celna"), ("immigration", "imigracja"),
        ("tourist", "turysta"), ("traveler", "podróżnik"),
        ("hotel", "hotel"), ("motel", "motel"), ("hostel", "hostel"),
        ("check-in", "zameldowanie"), ("check-out", "wymeldowanie"),
        ("elevator", "winda"), ("escalator", "schody ruchome"),
        ("staircase", "klatka schodowa"), ("hallway", "korytarz"),
        ("lobby", "hol"), ("reception", "recepcja"),
        
        # ── Documents & Paperwork ──
        ("receipt", "paragon"), ("invoice", "faktura"),
        ("contract", "umowa"), ("agreement", "porozumienie"),
        ("certificate", "certyfikat"), ("license", "pozwolenie"),
        ("application", "podanie"), ("form", "formularz"),
        ("signature", "podpis"), ("document", "dokument"),
        ("photocopy", "kserokopia"), ("printout", "wydruk"),
        ("envelope", "koperta"), ("stamp", "znaczek pocztowy"),
        ("package", "paczka"), ("parcel", "przesyłka"),
        ("mail", "poczta"), ("postcard", "pocztówka"),
        
        # ── Money & Finance ──
        ("salary", "pensja"), ("wage", "wynagrodzenie"),
        ("income", "dochód"), ("expense", "wydatek"),
        ("budget", "budżet"), ("savings", "oszczędności"),
        ("debt", "dług"), ("loan", "pożyczka"),
        ("mortgage", "hipoteka"), ("rent", "czynsz"),
        ("deposit", "zaliczka"), ("withdrawal", "wypłata"),
        ("bank account", "konto bankowe"), ("credit card", "karta kredytowa"),
        ("debit card", "karta debetowa"), ("cash", "gotówka"),
        ("coin", "moneta"), ("currency", "waluta"),
        ("exchange rate", "kurs wymiany"), ("interest", "odsetki"),
        ("tax", "podatek"), ("refund", "zwrot pieniędzy"),
        ("discount", "zniżka"), ("coupon", "kupon"),
        
        # ── Food & Cooking (non-CDI) ──
        ("recipe", "przepis"), ("ingredient", "składnik"),
        ("appetizer", "przystawka"), ("dessert", "deser"),
        ("beverage", "napój"), ("alcohol", "alkohol"),
        ("wine", "wino"), ("beer", "piwo"),
        ("whiskey", "whisky"), ("cocktail", "koktajl"),
        ("napkin", "serwetka"), ("tablecloth", "obrus"),
        ("cutlery", "sztućce"), ("chopsticks", "pałeczki"),
        ("corkscrew", "korkociąg"), ("ladle", "chochla"),
        ("colander", "durszlak"), ("whisk", "trzepaczka"),
        ("rolling pin", "wałek"), ("cutting board", "deska do krojenia"),
        ("leftovers", "resztki"), ("expiration date", "data ważności"),
        
        # ── Household (non-CDI) ──
        ("detergent", "detergent"), ("bleach", "wybielacz"),
        ("fabric softener", "płyn do zmiękczania"), ("stain remover", "odplamiacz"),
        ("furniture", "meble"), ("appliance", "urządzenie"),
        ("utensil", "przyrząd"), ("container", "pojemnik"),
        ("thermostat", "termostat"), ("fuse box", "skrzynka bezpiecznikowa"),
        ("extension cord", "przedłużacz"), ("power outlet", "gniazdko"),
        ("light bulb", "żarówka"), ("flashlight", "latarka"),
        ("toolbox", "skrzynka narzędziowa"), ("screwdriver", "śrubokręt"),
        ("wrench", "klucz francuski"), ("hammer", "młotek"),
        ("nail", "gwóźdź"), ("screw", "śruba"),
        ("bolt", "śruba z nakrętką"), ("nut", "nakrętka"),
        ("saw", "piła"), ("drill", "wiertarka"),
        ("sandpaper", "papier ścierny"), ("tape measure", "miarka"),
        ("level", "poziomica"), ("pliers", "szczypce"),
        ("hinge", "zawias"), ("latch", "zatrzask"),
        ("padlock", "kłódka"), ("combination lock", "zamek szyfrowy"),
        
        # ── Clothing & Accessories (non-CDI) ──
        ("tuxedo", "smoking"), ("gown", "suknia wieczorowa"),
        ("blazer", "marynarka"), ("cardigan", "rozpinany sweter"),
        ("turtleneck", "golf"), ("vest", "kamizelka"),
        ("cufflink", "spinka do mankietu"), ("brooch", "broszka"),
        ("pendant", "wisiorek"), ("anklet", "bransoletka na nogę"),
        ("cummerbund", "szarfa"), ("cravat", "fular"),
        ("loafer", "mokasyn"), ("stiletto", "szpilka"),
        ("clog", "chodak"), ("espadrille", "espardylla"),
        
        # ── Technology ──
        ("software", "oprogramowanie"), ("hardware", "sprzęt komputerowy"),
        ("database", "baza danych"), ("server", "serwer"),
        ("network", "sieć"), ("router", "router"),
        ("firewall", "zapora sieciowa"), ("virus", "wirus"),
        ("malware", "złośliwe oprogramowanie"), ("spam", "spam"),
        ("browser", "przeglądarka"), ("search engine", "wyszukiwarka"),
        ("download", "pobieranie"), ("upload", "wysyłanie"),
        ("file", "plik"), ("folder", "folder"),
        ("backup", "kopia zapasowa"), ("crash", "awaria"),
        ("bug", "błąd"), ("update", "aktualizacja"),
        ("version", "wersja"), ("beta", "beta"),
        ("username", "nazwa użytkownika"), ("password", "hasło"),
        ("login", "logowanie"), ("logout", "wylogowanie"),
        ("profile", "profil"), ("account", "konto"),
        ("settings", "ustawienia"), ("preferences", "preferencje"),
        ("notification", "powiadomienie"), ("alert", "alarm"),
        
        # ── Office & Work ──
        ("coworker", "współpracownik"), ("colleague", "kolega z pracy"),
        ("supervisor", "przełożony"), ("subordinate", "podwładny"),
        ("intern", "stażysta"), ("trainee", "praktykant"),
        ("promotion", "awans"), ("demotion", "degradacja"),
        ("resignation", "rezygnacja"), ("termination", "zwolnienie"),
        ("unemployment", "bezrobocie"), ("retirement", "emerytura"),
        ("pension", "emerytura"), ("benefits", "świadczenia"),
        ("overtime", "nadgodziny"), ("break", "przerwa"),
        ("commute", "dojazd do pracy"), ("telecommute", "praca zdalna"),
        ("memo", "notatka służbowa"), ("agenda", "porządek obrad"),
        ("minutes", "protokół"), ("presentation", "prezentacja"),
        ("spreadsheet", "arkusz kalkulacyjny"), ("slide", "slajd"),
        ("whiteboard", "tablica suchościeralna"), ("marker", "marker"),
        ("stapler", "zszywacz"), ("paperclip", "spinacz"),
        ("binder", "segregator"), ("folder", "teczka"),
        ("shredder", "niszczarka"), ("laminator", "laminator"),
        
        # ── Health & Medicine (non-CDI) ──
        ("prescription", "recepta"), ("pharmacist", "farmaceuta"),
        ("surgeon", "chirurg"), ("anesthesia", "znieczulenie"),
        ("diagnosis", "diagnoza"), ("prognosis", "rokowanie"),
        ("symptom", "objaw"), ("syndrome", "zespół"),
        ("chronic", "przewlekły"), ("acute", "ostry"),
        ("benign", "łagodny"), ("malignant", "złośliwy"),
        ("therapy", "terapia"), ("rehabilitation", "rehabilitacja"),
        ("vaccine", "szczepionka"), ("immunity", "odporność"),
        ("antibiotic", "antybiotyk"), ("painkiller", "środek przeciwbólowy"),
        ("dosage", "dawkowanie"), ("side effect", "efekt uboczny"),
        ("allergy", "alergia"), ("asthma", "astma"),
        ("diabetes", "cukrzyca"), ("hypertension", "nadciśnienie"),
        ("x-ray", "prześwietlenie"), ("MRI", "rezonans magnetyczny"),
        ("ultrasound", "USG"), ("biopsy", "biopsja"),
        ("cast", "gips"), ("crutches", "kule"),
        ("wheelchair", "wózek inwalidzki"), ("hearing aid", "aparat słuchowy"),
        ("pacemaker", "rozrusznik serca"), ("implant", "implant"),
        
        # ── Law & Crime ──
        ("attorney", "adwokat"), ("defendant", "oskarżony"),
        ("plaintiff", "powód"), ("witness", "świadek"),
        ("testimony", "zeznanie"), ("evidence", "dowód"),
        ("verdict", "werdykt"), ("appeal", "apelacja"),
        ("parole", "zwolnienie warunkowe"), ("probation", "okres próbny"),
        ("felony", "przestępstwo"), ("misdemeanor", "wykroczenie"),
        ("fraud", "oszustwo"), ("theft", "kradzież"),
        ("burglary", "włamanie"), ("robbery", "rabunek"),
        ("assault", "napaść"), ("homicide", "zabójstwo"),
        ("alibi", "alibi"), ("motive", "motyw"),
        ("handcuffs", "kajdanki"), ("interrogation", "przesłuchanie"),
        ("confession", "przyznanie się"), ("acquittal", "uniewinnienie"),
        
        # ── Politics & Government ──
        ("senator", "senator"), ("congressman", "kongresmen"),
        ("governor", "gubernator"), ("mayor", "burmistrz"),
        ("council", "rada"), ("committee", "komitet"),
        ("legislation", "ustawodawstwo"), ("regulation", "przepis"),
        ("amendment", "poprawka"), ("referendum", "referendum"),
        ("census", "spis ludności"), ("constituency", "okręg wyborczy"),
        ("campaign", "kampania"), ("candidate", "kandydat"),
        ("debate", "debata"), ("poll", "sondaż"),
        ("propaganda", "propaganda"), ("censorship", "cenzura"),
        ("diplomat", "dyplomata"), ("embassy", "ambasada"),
        ("consulate", "konsulat"), ("treaty", "traktat"),
        ("alliance", "sojusz"), ("coalition", "koalicja"),
        ("opposition", "opozycja"), ("majority", "większość"),
        ("minority", "mniejszość"), ("veto", "weto"),
        
        # ── Education (non-CDI) ──
        ("university", "uniwersytet"), ("college", "koledż"),
        ("semester", "semestr"), ("syllabus", "sylabus"),
        ("lecture", "wykład"), ("seminar", "seminarium"),
        ("dissertation", "dysertacja"), ("thesis", "praca dyplomowa"),
        ("professor", "profesor"), ("lecturer", "wykładowca"),
        ("dean", "dziekan"), ("chancellor", "rektor"),
        ("bachelor", "licencjat"), ("master", "magister"),
        ("doctorate", "doktorat"), ("postdoctoral", "postdoktorancki"),
        ("tuition", "czesne"), ("scholarship", "stypendium"),
        ("grant", "dotacja"), ("fellowship", "stypendium naukowe"),
        ("plagiarism", "plagiat"), ("citation", "cytat"),
        ("bibliography", "bibliografia"), ("footnote", "przypis"),
        
        # ── Arts & Entertainment ──
        ("exhibition", "wystawa"), ("gallery", "galeria"),
        ("sculpture", "rzeźba"), ("portrait", "portret"),
        ("landscape", "pejzaż"), ("still life", "martwa natura"),
        ("abstract", "abstrakcja"), ("masterpiece", "arcydzieło"),
        ("auction", "aukcja"), ("collector", "kolekcjoner"),
        ("symphony", "symfonia"), ("orchestra", "orkiestra"),
        ("conductor", "dyrygent"), ("composer", "kompozytor"),
        ("soprano", "sopran"), ("tenor", "tenor"),
        ("rehearsal", "próba"), ("performance", "występ"),
        ("applause", "aplauz"), ("standing ovation", "owacja na stojąco"),
        ("premiere", "premiera"), ("encore", "bis"),
        ("screenplay", "scenariusz"), ("script", "scenariusz"),
        ("rehearsal", "próba"), ("dress rehearsal", "próba generalna"),
        ("backstage", "za kulisami"), ("props", "rekwizyty"),
        ("costume", "kostium"), ("makeup", "makijaż"),
        
        # ── Religion ──
        ("religion", "religia"), ("faith", "wiara"),
        ("prayer", "modlitwa"), ("worship", "uwielbienie"),
        ("congregation", "kongregacja"), ("sermon", "kazanie"),
        ("priest", "ksiądz"), ("pastor", "pastor"),
        ("rabbi", "rabin"), ("imam", "imam"),
        ("monk", "mnich"), ("nun", "zakonnica"),
        ("saint", "święty"), ("prophet", "prorok"),
        ("pilgrim", "pielgrzym"), ("pilgrimage", "pielgrzymka"),
        ("baptism", "chrzest"), ("confession", "spowiedź"),
        ("communion", "komunia"), ("confirmation", "bierzmowanie"),
        ("funeral", "pogrzeb"), ("wedding", "ślub"),
        ("fasting", "post"), ("sacrifice", "ofiara"),
        ("altar", "ołtarz"), ("pew", "ławka kościelna"),
        ("stained glass", "witraż"), ("organ", "organy"),
        
        # ── Military ──
        ("army", "armia"), ("navy", "marynarka wojenna"),
        ("air force", "siły powietrzne"), ("marines", "piechota morska"),
        ("general", "generał"), ("colonel", "pułkownik"),
        ("sergeant", "sierżant"), ("lieutenant", "porucznik"),
        ("veteran", "weteran"), ("recruit", "rekrut"),
        ("artillery", "artyleria"), ("infantry", "piechota"),
        ("cavalry", "kawaleria"), ("tank", "czołg"),
        ("missile", "pocisk"), ("bomb", "bomba"),
        ("grenade", "granat"), ("landmine", "mina lądowa"),
        ("barracks", "koszary"), ("bunker", "bunkier"),
        ("trench", "okop"), ("camouflage", "kamuflaż"),
        ("medal", "medal"), ("decoration", "odznaczenie"),
        
        # ── Science ──
        ("theory", "teoria"), ("hypothesis", "hipoteza"),
        ("experiment", "eksperyment"), ("laboratory", "laboratorium"),
        ("specimen", "okaz"), ("sample", "próbka"),
        ("microscope", "mikroskop"), ("telescope", "teleskop"),
        ("particle", "cząstka"), ("atom", "atom"),
        ("molecule", "cząsteczka"), ("element", "pierwiastek"),
        ("compound", "związek chemiczny"), ("reaction", "reakcja"),
        ("gravity", "grawitacja"), ("friction", "tarcie"),
        ("velocity", "prędkość"), ("acceleration", "przyspieszenie"),
        ("evolution", "ewolucja"), ("species", "gatunek"),
        ("ecosystem", "ekosystem"), ("habitat", "siedlisko"),
        ("chromosome", "chromosom"), ("gene", "gen"),
        ("mutation", "mutacja"), ("clone", "klon"),
        
        # ── Abstract Concepts ──
        ("freedom", "wolność"), ("justice", "sprawiedliwość"),
        ("equality", "równość"), ("democracy", "demokracja"),
        ("liberty", "wolność"), ("tyranny", "tyrania"),
        ("courage", "odwaga"), ("cowardice", "tchórzostwo"),
        ("wisdom", "mądrość"), ("ignorance", "ignorancja"),
        ("knowledge", "wiedza"), ("truth", "prawda"),
        ("falsehood", "fałsz"), ("reality", "rzeczywistość"),
        ("illusion", "iluzja"), ("fantasy", "fantazja"),
        ("memory", "pamięć"), ("imagination", "wyobraźnia"),
        ("conscience", "sumienie"), ("consciousness", "świadomość"),
        ("destiny", "przeznaczenie"), ("coincidence", "zbieg okoliczności"),
        ("miracle", "cud"), ("disaster", "katastrofa"),
        ("chaos", "chaos"), ("harmony", "harmonia"),
        ("tradition", "tradycja"), ("innovation", "innowacja"),
        ("progress", "postęp"), ("decline", "upadek"),
        
        # =====================================================================
        # ANIMALS
        # =====================================================================
        ("dog", "pies"), ("cat", "kot"), ("bird", "ptak"), ("fish", "ryba"),
        ("horse", "koń"), ("cow", "krowa"), ("pig", "świnia"), ("sheep", "owca"),
        ("duck", "kaczka"), ("bear", "niedźwiedź"), ("mouse", "mysz"),
        ("rabbit", "królik"), ("frog", "żaba"), ("lion", "lew"),
        ("elephant", "słoń"), ("monkey", "małpa"), ("tiger", "tygrys"),
        ("goat", "koza"), ("chicken", "kurczak"), ("rooster", "kogut"),
        ("snake", "wąż"), ("turtle", "żółw"), ("bee", "pszczoła"),
        ("butterfly", "motyl"), ("ant", "mrówka"), ("spider", "pająk"),
        ("penguin", "pingwin"), ("whale", "wieloryb"), ("dolphin", "delfin"),
        ("donkey", "osioł"), ("turkey", "indyk"), ("fox", "lis"),
        ("wolf", "wilk"), ("deer", "jeleń"), ("zebra", "zebra"),
        ("giraffe", "żyrafa"), ("kangaroo", "kangur"), ("panda", "panda"),
        ("worm", "robak"), ("ladybug", "biedronka"), ("owl", "sowa"),
        ("parrot", "papuga"), ("puppy", "szczeniak"), ("kitten", "kociak"),
        ("hamster", "chomik"), ("squirrel", "wiewiórka"), ("hedgehog", "jeż"),
        ("crab", "krab"), ("octopus", "ośmiornica"), ("shark", "rekin"),
        ("crocodile", "krokodyl"), ("dinosaur", "dinozaur"), ("dragon", "smok"),
        ("unicorn", "jednorożec"), ("bat", "nietoperz"), ("rat", "szczur"),
        ("seal", "foka"), ("caterpillar", "gąsienica"), ("snail", "ślimak"),
        ("mosquito", "komar"), ("fly", "mucha"), ("beetle", "chrząszcz"),
        ("eagle", "orzeł"), ("hawk", "jastrząb"), ("falcon", "sokół"),
        ("swan", "łabędź"), ("peacock", "paw"), ("flamingo", "flaming"),
        ("leopard", "lampart"), ("cheetah", "gepard"), ("hyena", "hiena"),
        ("rhinoceros", "nosorożec"), ("hippopotamus", "hipopotam"), ("gorilla", "goryl"),
        ("chimpanzee", "szympans"), ("orangutan", "orangutan"), ("baboon", "pawian"),
        ("walrus", "mors"), ("otter", "wydra"), ("beaver", "bóbr"),
        ("porcupine", "jeżozwierz"), ("skunk", "skunks"), ("raccoon", "szop"),
        ("koala", "koala"), ("platypus", "dziobak"), ("armadillo", "pancernik"),

        # =====================================================================
        # ANIMAL SOUNDS
        # =====================================================================
        ("woof", "hau"), ("meow", "miau"), ("moo", "muu"),
        ("oink", "chrum"), ("baa", "bee"), ("quack", "kwa"),
        ("cluck", "ko"), ("neigh", "iiihaaa"), ("roar", "ryk"),
        ("chirp", "ćwir"), ("buzz", "bzzz"), ("ribbit", "kum"),
        ("growl", "warczeć"), ("howl", "wyć"), ("hiss", "syczeć"),
        ("squeak", "piszczeć"), ("cockadoodledoo", "kukuryku"), ("gobble", "gul gul"),

        # =====================================================================
        # FOOD AND DRINK
        # =====================================================================
        ("water", "woda"), ("milk", "mleko"), ("juice", "sok"), ("tea", "herbata"),
        ("coffee", "kawa"), ("bread", "chleb"), ("butter", "masło"),
        ("cheese", "ser"), ("egg", "jajko"), ("meat", "mięso"),
        ("apple", "jabłko"), ("banana", "banan"), ("cake", "ciasto"),
        ("soup", "zupa"), ("sugar", "cukier"), ("salt", "sól"),
        ("cookie", "ciastko"), ("candy", "cukierek"), ("chicken", "kurczak"),
        ("rice", "ryż"), ("pasta", "makaron"), ("potato", "ziemniak"),
        ("carrot", "marchewka"), ("tomato", "pomidor"), ("cucumber", "ogórek"),
        ("pea", "groszek"), ("bean", "fasola"), ("corn", "kukurydza"),
        ("strawberry", "truskawka"), ("cherry", "wiśnia"), ("grape", "winogrono"),
        ("orange", "pomarańcza"), ("lemon", "cytryna"), ("pear", "gruszka"),
        ("peach", "brzoskwinia"), ("chocolate", "czekolada"), ("ice cream", "lody"),
        ("pizza", "pizza"), ("hamburger", "hamburger"), ("sandwich", "kanapka"),
        ("breakfast", "śniadanie"), ("lunch", "obiad"), ("dinner", "kolacja"),
        ("pancake", "naleśnik"), ("honey", "miód"), ("jam", "dżem"),
        ("yogurt", "jogurt"), ("cereal", "płatki"), ("oatmeal", "owsianka"),
        ("sausage", "kiełbasa"), ("bacon", "bekon"), ("ham", "szynka"),
        ("shrimp", "krewetka"), ("lobster", "homar"), ("mushroom", "grzyb"),
        ("onion", "cebula"), ("garlic", "czosnek"), ("pepper", "papryka"),
        ("broccoli", "brokuł"), ("spinach", "szpinak"), ("celery", "seler"),
        ("avocado", "awokado"), ("pineapple", "ananas"), ("watermelon", "arbuz"),
        ("blueberry", "borówka"), ("raspberry", "malina"), ("coconut", "kokos"),
        ("nut", "orzech"), ("raisin", "rodzynek"), ("popcorn", "popcorn"),
        ("cracker", "krakers"), ("pretzel", "precel"), ("muffin", "muffin"),
        ("donut", "pączek"), ("pie", "placek"), ("ketchup", "keczup"),
        ("mustard", "musztarda"), ("mayonnaise", "majonez"), ("vinegar", "ocet"),
        ("oil", "olej"), ("flour", "mąka"), ("steak", "stek"),
        ("salad", "sałatka"), ("toast", "tost"), ("waffle", "gofr"),
        ("syrup", "syrop"), ("cream", "śmietana"), ("gravy", "sos pieczeniowy"),
        ("stew", "gulasz"), ("roast", "pieczeń"), ("grill", "grill"),
        ("spice", "przyprawa"), ("herb", "zioło"), ("cinnamon", "cynamon"),
        ("vanilla", "wanilia"), ("ginger", "imbir"), ("curry", "curry"),

        # =====================================================================
        # BODY PARTS
        # =====================================================================
        ("head", "głowa"), ("eye", "oko"), ("ear", "ucho"), ("nose", "nos"),
        ("mouth", "usta"), ("hand", "ręka"), ("foot", "stopa"), ("leg", "noga"),
        ("arm", "ramię"), ("finger", "palec"), ("tooth", "ząb"), ("hair", "włosy"),
        ("tongue", "język"), ("stomach", "brzuch"), ("back", "plecy"),
        ("knee", "kolano"), ("elbow", "łokieć"), ("chin", "broda"),
        ("cheek", "policzek"), ("forehead", "czoło"), ("thumb", "kciuk"),
        ("toe", "palec u nogi"), ("belly", "brzuszek"), ("bottom", "pupa"),
        ("neck", "szyja"), ("shoulder", "ramię"), ("face", "twarz"),
        ("ankle", "kostka"), ("wrist", "nadgarstek"), ("hip", "biodro"),
        ("lip", "warga"), ("eyebrow", "brew"), ("eyelash", "rzęsa"),
        ("jaw", "szczęka"), ("chest", "klatka piersiowa"), ("spine", "kręgosłup"),
        ("skin", "skóra"), ("bone", "kość"), ("muscle", "mięsień"),
        ("heart", "serce"), ("brain", "mózg"), ("lung", "płuco"),
        ("nail", "paznokieć"), ("freckle", "pieg"), ("mole", "pieprzyk"),
        ("belly button", "pępek"), ("nipple", "sutek"),
        ("nostril", "nozdrze"), ("gum", "dziąsło"), ("palate", "podniebienie"),
        ("artery", "tętnica"), ("vein", "żyła"), ("nerve", "nerw"),
        ("gland", "gruczoł"), ("tendon", "ścięgno"), ("ligament", "więzadło"),
        ("skull", "czaszka"), ("rib", "żebro"), ("pelvis", "miednica"),
        ("thigh", "udo"), ("calf", "łydka"), ("shin", "goleń"),
        ("palm", "dłoń"), ("knuckle", "kłykieć"), ("fingernail", "paznokieć"),

        # =====================================================================
        # FAMILY AND PEOPLE
        # =====================================================================
        ("mother", "matka"), ("father", "ojciec"), ("sister", "siostra"),
        ("brother", "brat"), ("baby", "dziecko"), ("grandma", "babcia"),
        ("grandpa", "dziadek"), ("aunt", "ciocia"), ("uncle", "wujek"),
        ("family", "rodzina"), ("daughter", "córka"), ("son", "syn"),
        ("parent", "rodzic"), ("cousin", "kuzyn"), ("grandchild", "wnuk"),
        ("husband", "mąż"), ("wife", "żona"), ("child", "dziecko"),
        ("friend", "przyjaciel"), ("neighbor", "sąsiad"), ("guest", "gość"),
        ("boy", "chłopiec"), ("girl", "dziewczynka"), ("man", "mężczyzna"),
        ("woman", "kobieta"), ("adult", "dorosły"), ("teenager", "nastolatek"),
        ("twin", "bliźniak"), ("niece", "siostrzenica"), ("nephew", "siostrzeniec"),
        ("bride", "panna młoda"), ("groom", "pan młody"), ("orphan", "sierota"),
        ("relative", "krewny"), ("ancestor", "przodek"), ("descendant", "potomek"),
        ("citizen", "obywatel"), ("foreigner", "cudzoziemiec"), ("stranger", "nieznajomy"),
        ("colleague", "kolega z pracy"), ("roommate", "współlokator"), ("landlord", "właściciel"),

        # =====================================================================
        # CLOTHING AND ACCESSORIES
        # =====================================================================
        ("shirt", "koszula"), ("pants", "spodnie"), ("shoes", "buty"),
        ("hat", "kapelusz"), ("coat", "płaszcz"), ("sock", "skarpetka"),
        ("dress", "sukienka"), ("jacket", "kurtka"), ("boot", "but"),
        ("sweater", "sweter"), ("glove", "rękawiczka"), ("scarf", "szalik"),
        ("pajamas", "piżama"), ("diaper", "pielucha"), ("bib", "śliniak"),
        ("belt", "pasek"), ("button", "guzik"), ("zipper", "zamek"),
        ("pocket", "kieszeń"), ("helmet", "kask"), ("uniform", "mundurek"),
        ("shorts", "szorty"), ("swimsuit", "strój kąpielowy"), ("bathrobe", "szlafrok"),
        ("slipper", "kapcie"), ("sneaker", "trampki"), ("sandal", "sandały"),
        ("tie", "krawat"), ("bow tie", "muszka"), ("suspenders", "szelki"),
        ("earring", "kolczyk"), ("necklace", "naszyjnik"), ("bracelet", "bransoletka"),
        ("ring", "pierścionek"), ("watch", "zegarek"), ("glasses", "okulary"),
        ("sunglasses", "okulary przeciwsłoneczne"), ("wallet", "portfel"), ("purse", "torebka"),
        ("backpack", "plecak"), ("umbrella", "parasol"), ("handkerchief", "chusteczka"),
        ("apron", "fartuch"), ("veil", "welon"), ("crown", "korona"),
        ("costume", "kostium"), ("mask", "maska"), ("wig", "peruka"),
        ("tuxedo", "smoking"), ("gown", "suknia wieczorowa"), ("blazer", "marynarka"),
        ("cardigan", "rozpinany sweter"), ("turtleneck", "golf"), ("polo", "koszulka polo"),
        ("tank top", "podkoszulek"), ("overalls", "kombinezon"), ("raincoat", "płaszcz przeciwdeszczowy"),

        # =====================================================================
        # HOUSEHOLD
        # =====================================================================
        ("house", "dom"), ("door", "drzwi"), ("window", "okno"),
        ("table", "stół"), ("chair", "krzesło"), ("bed", "łóżko"),
        ("kitchen", "kuchnia"), ("bathroom", "łazienka"), ("room", "pokój"),
        ("floor", "podłoga"), ("wall", "ściana"), ("clock", "zegar"),
        ("lamp", "lampa"), ("telephone", "telefon"), ("television", "telewizor"),
        ("book", "książka"), ("key", "klucz"), ("money", "pieniądze"),
        ("sofa", "kanapa"), ("carpet", "dywan"), ("curtain", "zasłona"),
        ("mirror", "lustro"), ("pillow", "poduszka"), ("blanket", "koc"),
        ("towel", "ręcznik"), ("soap", "mydło"), ("brush", "szczotka"),
        ("comb", "grzebień"), ("spoon", "łyżka"), ("fork", "widelec"),
        ("knife", "nóż"), ("plate", "talerz"), ("cup", "kubek"),
        ("glass", "szklanka"), ("bottle", "butelka"), ("bowl", "miska"),
        ("pot", "garnek"), ("pan", "patelnia"), ("fridge", "lodówka"),
        ("oven", "piekarnik"), ("sink", "zlew"), ("bathtub", "wanna"),
        ("toilet", "toaleta"), ("stairs", "schody"), ("roof", "dach"),
        ("yard", "podwórko"), ("garage", "garaż"), ("basement", "piwnica"),
        ("attic", "strych"), ("balcony", "balkon"), ("fence", "płot"),
        ("mailbox", "skrzynka pocztowa"), ("doormat", "wycieraczka"), ("doorknob", "klamka"),
        ("drawer", "szuflada"), ("shelf", "półka"), ("cabinet", "szafka"),
        ("closet", "szafa"), ("laundry", "pranie"), ("iron", "żelazko"),
        ("broom", "miotła"), ("mop", "mop"), ("bucket", "wiadro"),
        ("trash", "śmieci"), ("vacuum", "odkurzacz"), ("fan", "wentylator"),
        ("heater", "grzejnik"), ("air conditioner", "klimatyzacja"), ("candle", "świeca"),
        ("matches", "zapałki"), ("lighter", "zapalniczka"), ("battery", "bateria"),
        ("cushion", "poduszka"), ("mattress", "materac"), ("sheet", "prześcieradło"),
        ("quilt", "kołdra"), ("rug", "dywanik"), ("hanger", "wieszak"),
        ("extension cord", "przedłużacz"), ("power strip", "listwa zasilająca"),
        ("thermostat", "termostat"), ("smoke detector", "czujnik dymu"),
        ("fire extinguisher", "gaśnica"), ("first aid kit", "apteczka"),

        # =====================================================================
        # VEHICLES AND TRANSPORTATION
        # =====================================================================
        ("car", "samochód"), ("bus", "autobus"), ("train", "pociąg"),
        ("airplane", "samolot"), ("boat", "łódź"), ("bicycle", "rower"),
        ("truck", "ciężarówka"), ("motorcycle", "motocykl"), ("helicopter", "helikopter"),
        ("tractor", "traktor"), ("fire truck", "wóz strażacki"), ("ambulance", "ambulans"),
        ("scooter", "hulajnoga"), ("sled", "sanki"), ("wagon", "wózek"),
        ("taxi", "taksówka"), ("subway", "metro"), ("ferry", "prom"),
        ("sailboat", "żaglówka"), ("canoe", "kajak"), ("spaceship", "statek kosmiczny"),
        ("skateboard", "deskorolka"), ("roller skates", "wrotki"), ("tricycle", "rowerek"),
        ("minivan", "minivan"), ("limousine", "limuzyna"), ("bulldozer", "spychacz"),
        ("crane", "dźwig"), ("forklift", "wózek widłowy"), ("excavator", "koparka"),
        ("passenger", "pasażer"), ("driver", "kierowca"), ("pilot", "pilot"),
        ("captain", "kapitan"), ("conductor", "konduktor"), ("commuter", "dojeżdżający"),

        # =====================================================================
        # NATURE AND WEATHER
        # =====================================================================
        ("tree", "drzewo"), ("flower", "kwiat"), ("grass", "trawa"),
        ("sun", "słońce"), ("moon", "księżyc"), ("star", "gwiazda"),
        ("sky", "niebo"), ("rain", "deszcz"), ("snow", "śnieg"),
        ("river", "rzeka"), ("mountain", "góra"), ("forest", "las"),
        ("garden", "ogród"), ("fire", "ogień"), ("cloud", "chmura"),
        ("wind", "wiatr"), ("storm", "burza"), ("rainbow", "tęcza"),
        ("puddle", "kałuża"), ("mud", "błoto"), ("sand", "piasek"),
        ("rock", "kamień"), ("stick", "patyk"), ("leaf", "liść"),
        ("beach", "plaża"), ("ocean", "ocean"), ("lake", "jezioro"),
        ("island", "wyspa"), ("waterfall", "wodospad"), ("cave", "jaskinia"),
        ("volcano", "wulkan"), ("earthquake", "trzęsienie ziemi"), ("flood", "powódź"),
        ("fog", "mgła"), ("frost", "mróz"), ("ice", "lód"),
        ("hail", "grad"), ("lightning", "piorun"), ("thunder", "grzmot"),
        ("pond", "staw"), ("stream", "strumień"), ("bush", "krzak"),
        ("branch", "gałąź"), ("root", "korzeń"), ("seed", "nasiono"),
        ("petal", "płatek"), ("thorn", "kolec"), ("pollen", "pyłek"),
        ("avalanche", "lawina"), ("tornado", "tornado"), ("hurricane", "huragan"),
        ("drought", "susza"), ("blizzard", "zamieć"), ("monsoon", "monsun"),
        ("climate", "klimat"), ("atmosphere", "atmosfera"), ("horizon", "horyzont"),
        ("tide", "pływ"), ("current", "prąd"), ("wave", "fala"),
        ("cliff", "klif"), ("canyon", "kanion"), ("glacier", "lodowiec"),
        ("desert", "pustynia"), ("jungle", "dżungla"), ("swamp", "bagno"),

        # =====================================================================
        # COLORS
        # =====================================================================
        ("red", "czerwony"), ("blue", "niebieski"), ("green", "zielony"),
        ("yellow", "żółty"), ("white", "biały"), ("black", "czarny"),
        ("pink", "różowy"), ("purple", "fioletowy"), ("brown", "brązowy"),
        ("gray", "szary"), ("orange", "pomarańczowy"), ("silver", "srebrny"),
        ("gold", "złoty"), ("beige", "beżowy"), ("turquoise", "turkusowy"),
        ("navy blue", "granatowy"), ("lavender", "lawendowy"), ("maroon", "bordowy"),
        ("cream", "kremowy"), ("tan", "opalony"), ("rainbow", "tęczowy"),
        ("crimson", "karmazynowy"), ("scarlet", "szkarłatny"), ("indigo", "indygo"),
        ("magenta", "magenta"), ("cyan", "cyjan"), ("amber", "bursztynowy"),

        # =====================================================================
        # ADJECTIVES
        # =====================================================================
        ("big", "duży"), ("small", "mały"), ("hot", "gorący"), ("cold", "zimny"),
        ("good", "dobry"), ("bad", "zły"), ("new", "nowy"), ("old", "stary"),
        ("happy", "szczęśliwy"), ("sad", "smutny"), ("pretty", "ładny"),
        ("clean", "czysty"), ("dirty", "brudny"), ("fast", "szybki"),
        ("slow", "wolny"), ("loud", "głośny"), ("quiet", "cichy"),
        ("wet", "mokry"), ("dry", "suchy"), ("hard", "twardy"),
        ("soft", "miękki"), ("heavy", "ciężki"), ("light", "lekki"),
        ("full", "pełny"), ("empty", "pusty"), ("tall", "wysoki"),
        ("short", "niski"), ("long", "długi"), ("thick", "gruby"),
        ("thin", "cienki"), ("sweet", "słodki"), ("salty", "słony"),
        ("young", "młody"), ("hungry", "głodny"), ("thirsty", "spragniony"),
        ("tired", "zmęczony"), ("scared", "przestraszony"), ("angry", "zły"),
        ("sick", "chory"), ("brave", "odważny"), ("funny", "śmieszny"),
        ("beautiful", "piękny"), ("ugly", "brzydki"), ("rich", "bogaty"),
        ("poor", "biedny"), ("warm", "ciepły"), ("cool", "chłodny"),
        ("sharp", "ostry"), ("dull", "tępy"), ("smooth", "gładki"),
        ("rough", "szorstki"), ("sticky", "lepki"), ("slippery", "śliski"),
        ("tight", "ciasny"), ("loose", "luźny"), ("straight", "prosty"),
        ("crooked", "krzywy"), ("deep", "głęboki"), ("shallow", "płytki"),
        ("wide", "szeroki"), ("narrow", "wąski"), ("strong", "silny"),
        ("weak", "słaby"), ("clever", "mądry"), ("stupid", "głupi"),
        ("kind", "miły"), ("mean", "wredny"), ("polite", "grzeczny"),
        ("rude", "niegrzeczny"), ("honest", "uczciwy"), ("lazy", "leniwy"),
        ("busy", "zajęty"), ("free", "wolny"), ("safe", "bezpieczny"),
        ("dangerous", "niebezpieczny"), ("alive", "żywy"), ("dead", "martwy"),
        ("broken", "zepsuty"), ("lost", "zagubiony"), ("same", "taki sam"),
        ("different", "inny"), ("real", "prawdziwy"), ("fake", "fałszywy"),
        ("favorite", "ulubiony"), ("special", "specjalny"), ("normal", "normalny"),
        ("possible", "możliwy"), ("impossible", "niemożliwy"), ("necessary", "konieczny"),
        ("responsible", "odpowiedzialny"), ("available", "dostępny"), ("permanent", "trwały"),
        ("temporary", "tymczasowy"), ("ancient", "starożytny"), ("modern", "nowoczesny"),
        ("traditional", "tradycyjny"), ("official", "oficjalny"), ("private", "prywatny"),
        ("public", "publiczny"), ("legal", "legalny"), ("illegal", "nielegalny"),

        # =====================================================================
        # VERBS
        # =====================================================================
        ("eat", "jeść"), ("drink", "pić"), ("sleep", "spać"), ("run", "biegać"),
        ("walk", "chodzić"), ("sit", "siedzieć"), ("stand", "stać"),
        ("see", "widzieć"), ("hear", "słyszeć"), ("give", "dawać"),
        ("take", "brać"), ("open", "otwierać"), ("close", "zamykać"),
        ("read", "czytać"), ("write", "pisać"), ("sing", "śpiewać"),
        ("dance", "tańczyć"), ("swim", "pływać"), ("play", "bawić się"),
        ("wash", "myć"), ("cook", "gotować"), ("buy", "kupować"),
        ("speak", "mówić"), ("cry", "płakać"), ("laugh", "śmiać się"),
        ("smile", "uśmiechać się"), ("love", "kochać"), ("hate", "nienawidzić"),
        ("push", "pchać"), ("pull", "ciągnąć"), ("throw", "rzucać"),
        ("catch", "łapać"), ("kick", "kopać"), ("jump", "skakać"),
        ("fly", "latać"), ("climb", "wspinać się"), ("fall", "spadać"),
        ("break", "łamać"), ("cut", "ciąć"), ("draw", "rysować"),
        ("paint", "malować"), ("build", "budować"), ("hide", "chować"),
        ("find", "znaleźć"), ("help", "pomagać"), ("kiss", "całować"),
        ("hug", "przytulać"), ("wake", "budzić"), ("bite", "gryźć"),
        ("think", "myśleć"), ("know", "wiedzieć"), ("remember", "pamiętać"),
        ("forget", "zapominać"), ("learn", "uczyć się"), ("teach", "uczyć"),
        ("ask", "pytać"), ("answer", "odpowiadać"), ("tell", "mówić"),
        ("listen", "słuchać"), ("wait", "czekać"), ("hurry", "śpieszyć się"),
        ("stop", "zatrzymać"), ("start", "zaczynać"), ("finish", "kończyć"),
        ("try", "próbować"), ("win", "wygrać"), ("lose", "przegrać"),
        ("bring", "przynosić"), ("carry", "nieść"), ("drop", "upuszczać"),
        ("pick up", "podnosić"), ("put", "kłaść"), ("hold", "trzymać"),
        ("touch", "dotykać"), ("feel", "czuć"), ("smell", "wąchać"),
        ("taste", "smakować"), ("watch", "oglądać"), ("show", "pokazywać"),
        ("call", "dzwonić"), ("send", "wysyłać"), ("receive", "dostawać"),
        ("come", "przychodzić"), ("go", "iść"), ("enter", "wchodzić"),
        ("leave", "wychodzić"), ("arrive", "przyjeżdżać"), ("return", "wracać"),
        ("live", "mieszkać"), ("die", "umierać"), ("grow", "rosnąć"),
        ("explain", "wyjaśniać"), ("describe", "opisywać"), ("compare", "porównywać"),
        ("suggest", "sugerować"), ("recommend", "polecać"), ("warn", "ostrzegać"),
        ("promise", "obiecywać"), ("agree", "zgadzać się"), ("refuse", "odmawiać"),
        ("allow", "pozwalać"), ("forbid", "zabraniać"), ("force", "zmuszać"),
        ("manage", "zarządzać"), ("organize", "organizować"), ("prepare", "przygotowywać"),
        ("investigate", "badać"), ("discover", "odkrywać"), ("invent", "wynajdywać"),
        ("translate", "tłumaczyć"), ("pronounce", "wymawiać"), ("spell", "literować"),

        # =====================================================================
        # PLACES AND BUILDINGS
        # =====================================================================
        ("school", "szkoła"), ("store", "sklep"), ("hospital", "szpital"),
        ("church", "kościół"), ("park", "park"), ("street", "ulica"),
        ("city", "miasto"), ("country", "kraj"), ("airport", "lotnisko"),
        ("restaurant", "restauracja"), ("library", "biblioteka"), ("museum", "muzeum"),
        ("zoo", "zoo"), ("playground", "plac zabaw"), ("swimming pool", "basen"),
        ("movie theater", "kino"), ("supermarket", "supermarket"), ("pharmacy", "apteka"),
        ("bank", "bank"), ("post office", "poczta"), ("police station", "komisariat"),
        ("fire station", "remiza"), ("gas station", "stacja benzynowa"), ("hotel", "hotel"),
        ("factory", "fabryka"), ("office", "biuro"), ("farm", "farma"),
        ("bakery", "piekarnia"), ("butcher", "rzeźnik"), ("market", "rynek"),
        ("stadium", "stadion"), ("gym", "siłownia"), ("court", "sąd"),
        ("prison", "więzienie"), ("cemetery", "cmentarz"), ("temple", "świątynia"),
        ("mosque", "meczet"), ("synagogue", "synagoga"), ("cathedral", "katedra"),
        ("embassy", "ambasada"), ("university", "uniwersytet"), ("college", "koledż"),
        ("laboratory", "laboratorium"), ("observatory", "obserwatorium"), ("warehouse", "magazyn"),
        ("skyscraper", "drapacz chmur"), ("monument", "pomnik"), ("fountain", "fontanna"),
        ("bridge", "most"), ("tunnel", "tunel"), ("highway", "autostrada"),

        # =====================================================================
        # TIME
        # =====================================================================
        ("day", "dzień"), ("night", "noc"), ("morning", "rano"),
        ("today", "dziś"), ("yesterday", "wczoraj"), ("tomorrow", "jutro"),
        ("week", "tydzień"), ("month", "miesiąc"), ("year", "rok"),
        ("summer", "lato"), ("winter", "zima"), ("spring", "wiosna"),
        ("autumn", "jesień"), ("birthday", "urodziny"), ("holiday", "święta"),
        ("minute", "minuta"), ("hour", "godzina"), ("second", "sekunda"),
        ("Monday", "poniedziałek"), ("Tuesday", "wtorek"), ("Wednesday", "środa"),
        ("Thursday", "czwartek"), ("Friday", "piątek"), ("Saturday", "sobota"),
        ("Sunday", "niedziela"), ("weekend", "weekend"), ("decade", "dekada"),
        ("century", "wiek"), ("millennium", "tysiąclecie"), ("era", "era"),
        ("deadline", "termin"), ("schedule", "harmonogram"), ("appointment", "spotkanie"),
        ("calendar", "kalendarz"), ("alarm", "budzik"), ("timer", "zegar"),

        # =====================================================================
        # TOYS AND PLAY
        # =====================================================================
        ("toy", "zabawka"), ("ball", "piłka"), ("doll", "lalka"),
        ("block", "klocki"), ("puzzle", "puzzle"), ("game", "gra"),
        ("swing", "huśtawka"), ("slide", "zjeżdżalnia"), ("balloon", "balon"),
        ("bubble", "bańka"), ("crayon", "kredka"), ("pencil", "ołówek"),
        ("pen", "długopis"), ("paper", "papier"), ("scissors", "nożyczki"),
        ("glue", "klej"), ("paint", "farba"), ("playdough", "plastelina"),
        ("teddy bear", "miś"), ("rattle", "grzechotka"), ("stuffed animal", "pluszak"),
        ("whistle", "gwizdek"), ("drum", "bęben"), ("xylophone", "ksylofon"),
        ("trumpet", "trąbka"), ("guitar", "gitara"), ("piano", "pianino"),
        ("marble", "szklana kulka"), ("kite", "latawiec"), ("yo-yo", "jojo"),
        ("jump rope", "skakanka"), ("hula hoop", "hula hop"), ("frisbee", "frisbee"),
        ("cards", "karty"), ("domino", "domino"), ("chess", "szachy"),
        ("checkers", "warcaby"), ("dice", "kostki"), ("sword", "miecz"),
        ("magic wand", "różdżka"), ("tent", "namiot"), ("fort", "forteca"),
        ("board game", "gra planszowa"), ("video game", "gra wideo"),
        ("puppet", "pacynka"), ("marionette", "marionetka"),

        # =====================================================================
        # PRONOUNS AND FUNCTION WORDS
        # =====================================================================
        ("I", "ja"), ("you", "ty"), ("he", "on"), ("she", "ona"),
        ("we", "my"), ("they", "oni"), ("me", "mnie"),
        ("my", "mój"), ("your", "twój"), ("our", "nasz"),
        ("this", "to"), ("that", "tamto"), ("here", "tutaj"),
        ("there", "tam"), ("who", "kto"), ("what", "co"),
        ("where", "gdzie"), ("when", "kiedy"), ("why", "dlaczego"),
        ("how", "jak"), ("yes", "tak"), ("no", "nie"),
        ("please", "proszę"), ("thank you", "dziękuję"), ("sorry", "przepraszam"),
        ("and", "i"), ("but", "ale"), ("or", "albo"),
        ("because", "bo"), ("with", "z"), ("without", "bez"),

        # =====================================================================
        # NUMBERS
        # =====================================================================
        ("one", "jeden"), ("two", "dwa"), ("three", "trzy"),
        ("four", "cztery"), ("five", "pięć"), ("six", "sześć"),
        ("seven", "siedem"), ("eight", "osiem"), ("nine", "dziewięć"),
        ("ten", "dziesięć"), ("eleven", "jedenaście"), ("twelve", "dwanaście"),
        ("thirteen", "trzynaście"), ("fourteen", "czternaście"), ("fifteen", "piętnaście"),
        ("sixteen", "szesnaście"), ("seventeen", "siedemnaście"), ("eighteen", "osiemnaście"),
        ("nineteen", "dziewiętnaście"), ("twenty", "dwadzieścia"),
        ("hundred", "sto"), ("thousand", "tysiąc"), ("million", "milion"),

        # =====================================================================
        # EMOTIONS AND STATES
        # =====================================================================
        ("happy", "szczęśliwy"), ("sad", "smutny"), ("angry", "zły"),
        ("scared", "przestraszony"), ("surprised", "zaskoczony"), ("bored", "znudzony"),
        ("excited", "podekscytowany"), ("worried", "zmartwiony"), ("jealous", "zazdrosny"),
        ("proud", "dumny"), ("embarrassed", "zawstydzony"), ("confused", "zdezorientowany"),
        ("lonely", "samotny"), ("calm", "spokojny"), ("nervous", "nerwowy"),
        ("shy", "nieśmiały"), ("curious", "ciekawy"), ("grateful", "wdzięczny"),
        ("guilty", "winny"), ("ashamed", "zawstydzony"), ("hopeful", "pełen nadziei"),
        ("disappointed", "rozczarowany"), ("frustrated", "sfrustrowany"), ("relieved", "ulżony"),
        ("anxious", "niespokojny"), ("depressed", "przygnębiony"), ("enthusiastic", "entuzjastyczny"),

        # =====================================================================
        # PROFESSIONS
        # =====================================================================
        ("doctor", "lekarz"), ("nurse", "pielęgniarka"), ("teacher", "nauczyciel"),
        ("police officer", "policjant"), ("firefighter", "strażak"), ("soldier", "żołnierz"),
        ("pilot", "pilot"), ("chef", "szef kuchni"), ("farmer", "rolnik"),
        ("dentist", "dentysta"), ("vet", "weterynarz"), ("singer", "piosenkarz"),
        ("actor", "aktor"), ("artist", "artysta"), ("writer", "pisarz"),
        ("scientist", "naukowiec"), ("astronaut", "astronauta"), ("judge", "sędzia"),
        ("lawyer", "prawnik"), ("priest", "ksiądz"), ("clown", "klaun"),
        ("magician", "magik"), ("pirate", "pirat"), ("architect", "architekt"),
        ("engineer", "inżynier"), ("programmer", "programista"), ("journalist", "dziennikarz"),
        ("photographer", "fotograf"), ("musician", "muzyk"), ("composer", "kompozytor"),
        ("director", "reżyser"), ("producer", "producent"), ("editor", "redaktor"),
        ("accountant", "księgowy"), ("manager", "kierownik"), ("assistant", "asystent"),
        ("secretary", "sekretarka"), ("receptionist", "recepcjonista"), ("cashier", "kasjer"),
        ("mechanic", "mechanik"), ("plumber", "hydraulik"), ("electrician", "elektryk"),
        ("carpenter", "stolarz"), ("painter", "malarz"), ("sculptor", "rzeźbiarz"),
        ("philosopher", "filozof"), ("historian", "historyk"), ("economist", "ekonomista"),
        ("politician", "polityk"), ("diplomat", "dyplomata"), ("ambassador", "ambasador"),

        # =====================================================================
        # MEDICAL
        # =====================================================================
        ("medicine", "lekarstwo"), ("bandaid", "plaster"), ("thermometer", "termometr"),
        ("syringe", "strzykawka"), ("pill", "tabletka"), ("vitamin", "witamina"),
        ("cough", "kaszel"), ("sneeze", "kichanie"), ("fever", "gorączka"),
        ("headache", "ból głowy"), ("stomachache", "ból brzucha"), ("earache", "ból ucha"),
        ("surgery", "operacja"), ("injection", "zastrzyk"), ("prescription", "recepta"),
        ("diagnosis", "diagnoza"), ("symptom", "objaw"), ("treatment", "leczenie"),
        ("allergy", "alergia"), ("infection", "infekcja"), ("disease", "choroba"),
        ("ambulance", "ambulans"), ("emergency", "nagły wypadek"), ("patient", "pacjent"),

        # =====================================================================
        # TECHNOLOGY AND MEDIA
        # =====================================================================
        ("computer", "komputer"), ("laptop", "laptop"), ("keyboard", "klawiatura"),
        ("mouse", "myszka"), ("screen", "ekran"), ("printer", "drukarka"),
        ("internet", "internet"), ("website", "strona internetowa"), ("email", "email"),
        ("password", "hasło"), ("software", "oprogramowanie"), ("hardware", "sprzęt"),
        ("camera", "aparat"), ("radio", "radio"), ("television", "telewizor"),
        ("newspaper", "gazeta"), ("magazine", "czasopismo"), ("article", "artykuł"),
        ("headline", "nagłówek"), ("advertisement", "reklama"), ("channel", "kanał"),
        ("microphone", "mikrofon"), ("speaker", "głośnik"), ("headphones", "słuchawki"),
        ("charger", "ładowarka"), ("cable", "kabel"), ("router", "router"),
        ("satellite", "satelita"), ("antenna", "antena"), ("signal", "sygnał"),

        # =====================================================================
        # POLITICS AND LAW
        # =====================================================================
        ("government", "rząd"), ("president", "prezydent"), ("election", "wybory"),
        ("vote", "głos"), ("law", "prawo"), ("court", "sąd"),
        ("judge", "sędzia"), ("jury", "ława przysięgłych"), ("trial", "proces"),
        ("crime", "przestępstwo"), ("prisoner", "więzień"), ("sentence", "wyrok"),
        ("constitution", "konstytucja"), ("parliament", "parlament"), ("senate", "senat"),
        ("tax", "podatek"), ("budget", "budżet"), ("policy", "polityka"),
        ("treaty", "traktat"), ("alliance", "sojusz"), ("embargo", "embargo"),

        # =====================================================================
        # BUSINESS AND ECONOMY
        # =====================================================================
        ("company", "firma"), ("business", "biznes"), ("industry", "przemysł"),
        ("trade", "handel"), ("import", "import"), ("export", "eksport"),
        ("profit", "zysk"), ("loss", "strata"), ("salary", "pensja"),
        ("invoice", "faktura"), ("receipt", "paragon"), ("contract", "umowa"),
        ("customer", "klient"), ("employee", "pracownik"), ("employer", "pracodawca"),
        ("meeting", "spotkanie"), ("conference", "konferencja"), ("deadline", "termin"),
        ("stock market", "giełda"), ("investment", "inwestycja"), ("interest rate", "stopa procentowa"),

        # =====================================================================
        # EDUCATION
        # =====================================================================
        ("student", "uczeń"), ("exam", "egzamin"), ("homework", "praca domowa"),
        ("lesson", "lekcja"), ("semester", "semestr"), ("diploma", "dyplom"),
        ("degree", "stopień naukowy"), ("scholarship", "stypendium"), ("tuition", "czesne"),
        ("textbook", "podręcznik"), ("notebook", "zeszyt"), ("chalk", "kreda"),
        ("blackboard", "tablica"), ("principal", "dyrektor szkoły"), ("professor", "profesor"),
        ("lecture", "wykład"), ("seminar", "seminarium"), ("curriculum", "program nauczania"),

        # =====================================================================
        # OTHER COMMON NON-CDI WORDS
        # =====================================================================
        ("name", "imię"), ("gift", "prezent"), ("music", "muzyka"),
        ("story", "historia"), ("picture", "obrazek"), ("animal", "zwierzę"),
        ("food", "jedzenie"), ("king", "król"), ("queen", "królowa"),
        ("princess", "księżniczka"), ("prince", "książę"), ("monster", "potwór"),
        ("ghost", "duch"), ("angel", "anioł"), ("robot", "robot"),
        ("rocket", "rakieta"), ("castle", "zamek"), ("stroller", "wózek"),
        ("crib", "łóżeczko"), ("pacifier", "smoczek"), ("diaper", "pielucha"),
        ("magic", "magia"), ("treasure", "skarb"), ("adventure", "przygoda"),
        ("secret", "tajemnica"), ("surprise", "niespodzianka"), ("mistake", "błąd"),
        ("accident", "wypadek"), ("danger", "niebezpieczeństwo"), ("flag", "flaga"),
        ("map", "mapa"), ("letter", "list"), ("envelope", "koperta"),
        ("stamp", "znaczek"), ("package", "paczka"), ("wheel", "koło"),
        ("engine", "silnik"), ("machine", "maszyna"), ("tool", "narzędzie"),
        ("weapon", "broń"), ("shield", "tarcza"), ("ladder", "drabina"),
        ("rope", "lina"), ("chain", "łańcuch"), ("magnet", "magnes"),
        ("microscope", "mikroskop"), ("telescope", "teleskop"), ("light", "światło"),
        ("shadow", "cień"), ("hole", "dziura"), ("dot", "kropka"),
        ("line", "linia"), ("circle", "koło"), ("square", "kwadrat"),
        ("triangle", "trójkąt"), ("cross", "krzyż"), ("arrow", "strzałka"),
        ("half", "połowa"), ("whole", "całość"), ("part", "część"),
        ("piece", "kawałek"), ("pair", "para"), ("group", "grupa"),
        ("crowd", "tłum"), ("team", "zespół"), ("war", "wojna"),
        ("peace", "pokój"), ("victory", "zwycięstwo"), ("defeat", "porażka"),
        ("race", "wyścig"), ("competition", "konkurs"), ("party", "przyjęcie"),
        ("celebration", "świętowanie"), ("parade", "parada"), ("circus", "cyrk"),
        ("fair", "jarmark"), ("carnival", "karnawał"), ("passport", "paszport"),
        ("visa", "wiza"), ("ticket", "bilet"), ("luggage", "bagaż"),
        ("suitcase", "walizka"), ("reservation", "rezerwacja"), ("itinerary", "plan podróży"),
        ("elevator", "winda"), ("escalator", "schody ruchome"), ("ramp", "podjazd"),
        ("receipt", "paragon"), ("guarantee", "gwarancja"), ("refund", "zwrot pieniędzy"),
        ("manual", "instrukcja"), ("warranty", "gwarancja"), ("insurance", "ubezpieczenie"),
    ]
    
    return pairs


def write_dictionary(pairs, output_path):
    grouped = defaultdict(list)
    for en, pl in pairs:
        grouped[en.lower()].append(pl.lower())
    
    for en_word in grouped:
        grouped[en_word] = list(set(grouped[en_word]))
    
    with open(output_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(["english", "polish", "polish_alternatives"])
        
        for en_word in sorted(grouped.keys()):
            pl_words = grouped[en_word]
            main_pl = pl_words[0]
            alternatives = "|".join(pl_words[1:]) if len(pl_words) > 1 else ""
            writer.writerow([en_word, main_pl, alternatives])
    
    print(f"  Written {len(grouped)} entries to {output_path}")


if __name__ == "__main__":
    os.makedirs(DATA_DIR, exist_ok=True)
    
    print("Building expanded Polish-English dictionary...")
    pairs = build_builtin_dictionary()
    print(f"  Built {len(pairs)} word pairs")
    
    write_dictionary(pairs, DICT_OUT)
    
    print(f"\nDone! Dictionary saved to: {DICT_OUT}")
    
    with open(DICT_OUT, 'r', encoding='utf-8') as f:
        total = sum(1 for _ in f) - 1
    print(f"Total unique English entries: {total}")