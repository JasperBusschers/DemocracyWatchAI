class LanguageManager:
    """
    Manages the available languages and their corresponding translations.
    """
    def __init__(self):
        self.languages = {
            'en': 'English',
            'nl': 'Nederlands',
            'fr': 'Français'
        }
        self.translations = {
            'en': {
                'title': 'Democracy Watch',
                'header_title': 'Democracy Watch',
                'donate_button': 'Donate',
                'login_button': 'Login',
                'mission_title': 'Our Mission',
                'mission_text': 'We believe for democracy to work, people need to be aware of what happened that is relevant to them. But nobody has time to follow all debates. This is what we provide, an AI that follows the democracy, so you can focus on what matters which is giving your opinion.',
                'regions_title': 'Available Regions',
                'how_it_works_title': 'How We Make Democracy More Accessible',
                'how_it_works': [
                    {
                        'number': '1',
                        'title': 'Trusted Sources',
                        'description': 'We carefully gather and verify transcripts from official parliamentary debates and public political discussions to ensure accuracy and authenticity.'
                    },
                    {
                        'number': '2',
                        'title': 'Knowledge Network',
                        'description': 'Our system creates comprehensive connections between speakers, topics, and statements, mapping the complex landscape of democratic discourse.'
                    },
                    {
                        'number': '3',
                        'title': 'Instant Insights',
                        'description': 'Access clear, factual information about democratic debates through our AI chatbot, making political transparency just a question away.'
                    }
                ],
                'regions': [
                    {'EN_name': 'Belgium', 'name': 'Belgium', 'status': 'Flanders only', 'flag': 'https://flagcdn.com/w320/be.png'},
                    {'EN_name': 'Netherlands', 'name': 'Netherlands', 'status': 'In progress', 'flag': 'https://flagcdn.com/w320/nl.png'},
                    {'EN_name': 'Europe', 'name': 'Europe', 'status': 'In progress', 'flag': 'https://flagcdn.com/w320/eu.png'}
                ],
                'footer_text': '© 2023 Democracy Watch. Completely open source.',
                'login_title': 'Login to Democracy Watch',
                'login_message': 'Choose your preferred login method:',
                'close': 'Close',
                'settings': 'Settings',
                'logout': 'Logout',
                'example_questions': [
                    {'question': 'What was recently discussed?', 'text': 'What was recently discussed?', 'icon': '🏛️'},
                    {'question': 'What was said relevant for me?', 'text': 'What was said relevant for me?', 'icon': '📊'},
                    {'question': 'What was mentioned about climate change?', 'text': 'What was mentioned about climate change?', 'icon': '🌍'}
                ],
                'input_placeholder': 'Ask a question...',
                'send_button': 'Send',
                'sidebar_title': 'Statement Details',
                'previous_button': 'Previous',
                'next_button': 'Next',
                'show_history': 'History',
                'welcome_msg': 'Welcome to Democracy Watch! How can I assist you today?'

            },
            'nl': {
                'title': 'Democracy Watch',
                'header_title': 'Democracy Watch',
                'donate_button': 'Doneren',
                'login_button': 'Inloggen',
                'mission_title': 'Onze Missie',
                'mission_text': 'Wij geloven dat voor een werkende democratie mensen op de hoogte moeten zijn van wat er relevant voor hen is gebeurd. Maar niemand heeft tijd om alle debatten te volgen. Dit is wat wij bieden: een AI die de democratie volgt, zodat u zich kunt concentreren op wat belangrijk is, namelijk het geven van uw mening.',
                'regions_title': 'Beschikbare Regios',
                'how_it_works_title': 'Hoe We Democratie Toegankelijker Maken',
                'how_it_works': [
                    {
                        'number': '1',
                        'title': 'Betrouwbare Bronnen',
                        'description': 'Wij verzamelen en verifiëren zorgvuldig transcripties van officiële parlementaire debatten en publieke politieke discussies om nauwkeurigheid en authenticiteit te garanderen.'
                    },
                    {
                        'number': '2',
                        'title': 'Kennisnetwerk',
                        'description': 'Ons systeem creëert uitgebreide verbindingen tussen sprekers, onderwerpen en verklaringen, waardoor het complexe landschap van democratische discussies in kaart wordt gebracht.'
                    },
                    {
                        'number': '3',
                        'title': 'Directe Inzichten',
                        'description': 'Toegang tot duidelijke, feitelijke informatie over democratische debatten via onze AI-chatbot, waardoor politieke transparantie slechts een vraag verwijderd is.'
                    }
                ],
                'regions': [
                    {'EN_name': 'Belgium', 'name': 'België', 'status': 'Alleen Vlaanderen', 'flag': 'https://flagcdn.com/w320/be.png'},
                    {'EN_name': 'Netherlands', 'name': 'Nederland', 'status': 'In progress', 'flag': 'https://flagcdn.com/w320/nl.png'},
                    {'EN_name': 'Europe', 'name': 'Europa', 'status': 'In progress', 'flag': 'https://flagcdn.com/w320/eu.png'}
                ],
                'footer_text': '© 2023 Democracy Watch. Volledig open source.',
                'login_title': 'Inloggen bij Democracy Watch',
                'login_message': 'Kies uw gewenste inlogmethode:',
                'close': 'Sluiten',
                'settings': 'Instellingen',
                'logout': 'Uitloggen',
                'example_questions': [
                    {'question': 'Wat is democratie?', 'text': 'Wat is democratie?', 'icon': '🏛️'},
                    {'question': 'Recente stemmen', 'text': 'Recente stemmen', 'icon': '📊'},
                    {'question': 'Klimaatveranderingsverklaringen', 'text': 'Klimaatveranderingsverklaringen', 'icon': '🌍'}
                ],
                'input_placeholder': 'Stel een vraag...',
                'send_button': 'Verstuur',
                'sidebar_title': 'Verklaring Details',
                'previous_button': 'Vorige',
                'next_button': 'Volgende',
                'show_history':'Chat geschiedenis',
                'welcome_msg': 'Welcome bij Democracy Watch! Hoe kan ik u vandaag helpen?'

            },
            'fr': {
                'title': 'Democracy Watch',
                'header_title': 'Democracy Watch',
                'donate_button': 'Faire un don',
                'login_button': 'Connexion',
                'mission_title': 'Notre Mission',
                'mission_text': 'Nous croyons que pour que la démocratie fonctionne, les gens doivent être conscients de ce qui s\'est passé et qui les concerne. Mais personne n\'a le temps de suivre tous les débats. C\'est ce que nous fournissons : une IA qui suit la démocratie, afin que vous puissiez vous concentrer sur ce qui compte, c\'est-à-dire donner votre opinion.',
                'regions_title': 'Régions Disponibles',
                'how_it_works_title': 'Comment Nous Rendons La Démocratie Plus Accessible',
                'how_it_works': [
                    {
                        'number': '1',
                        'title': 'Sources Fiables',
                        'description': 'Nous collectons et vérifions soigneusement les transcriptions des débats parlementaires officiels et des discussions politiques publiques pour garantir leur exactitude et leur authenticité.'
                    },
                    {
                        'number': '2',
                        'title': 'Réseau de Connaissances',
                        'description': 'Notre système crée des connexions complètes entre les intervenants, les sujets et les déclarations, cartographiant le paysage complexe du discours démocratique.'
                    },
                    {
                        'number': '3',
                        'title': 'Informations Instantanées',
                        'description': 'Accédez à des informations claires et factuelles sur les débats démocratiques grâce à notre chatbot IA, rendant la transparence politique à portée de main.'
                    }
                ],
                'regions': [
                    {'EN_name': 'Belgium', 'name': 'Belgique', 'status': 'Flandre uniquement', 'flag': 'https://flagcdn.com/w320/be.png'},
                    {'EN_name': 'Netherlands', 'name': 'Pays-Bas', 'status': 'In progress', 'flag': 'https://flagcdn.com/w320/nl.png'},
                    {'EN_name': 'Europe', 'name': 'Europe', 'status': 'En cours', 'In progress': 'https://flagcdn.com/w320/eu.png'}
                ],
                'footer_text': '© 2023 Democracy Watch. Tous droits réservés.',
                'login_title': 'Connexion à Democracy Watch',
                'login_message': 'Choisissez votre méthode de connexion préférée:',
                'close': 'Fermer',
                'settings': 'Paramètres',
                'logout': 'Déconnexion',
                'example_questions': [
                    {'question': 'Qu\'est-ce que la démocratie?', 'text': 'Qu\'est-ce que la démocratie?', 'icon': '🏛️'},
                    {'question': 'Votes récents', 'text': 'Votes récents', 'icon': '📊'},
                    {'question': 'Déclarations sur le changement climatique', 'text': 'Déclarations sur le changement climatique', 'icon': '🌍'}
                ],
                'input_placeholder': 'Posez une question...',
                'send_button': 'Envoyer',
                'sidebar_title': 'Détails de la Déclaration',
                'previous_button': 'Précédent',
                'next_button': 'Suivant',
                'show_history':'Histoire',
                'welcome_msg':'Bienvenue sur Démocratie en surveillance ! Comment puis-je vous aider aujourd\'hui ?'
            }
        }

    def get_language_name(self, lang_code):
        return self.languages.get(lang_code, None)

    def get_translations(self, lang_code):
        return self.translations.get(lang_code, self.translations['en'])

    def get_available_languages(self):
        return self.languages

    def is_language_supported(self, lang_code):
        return lang_code in self.languages
