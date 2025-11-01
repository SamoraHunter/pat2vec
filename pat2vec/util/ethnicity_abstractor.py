import pandas as pd


"""The script is designed to process a column of free text entries in a CSV file under an "ethnicity" field. Its goal is to match these entries with corresponding categories from the UK census.

It references groups outlined in the style guide provided by https://www.ethnicity-facts-figures.service.gov.uk/style-guide/ethnic-groups. The script doesn't use fuzzy matching, but it allows for handling edge cases by adding specific conditions within the script.

To improve accuracy, certain country groups have been adjusted to ensure more precise mappings. The script assumes each country contains a single ethnic group for simplicity, categorizing them under the default ethnic group unless the user specifies otherwise (e.g., "Asian Caribbean").

It's important to note that both the categorization lists and output lists may contain errors and ambiguities, so manual review of the script's outputs is necessary.

Additionally, if a user explicitly specifies a racial group (e.g., "White"), it takes precedence over the national origin term provided in the entry."""


class EthnicityAbstractor:
    @staticmethod
    def abstractEthnicity(
        dataFrame: pd.DataFrame, outputNameString: str, ethnicityColumnString: str
    ) -> pd.DataFrame:
        """Abstracts ethnicity from free text to UK census categories.

        This method processes a DataFrame column containing free-text ethnicity
        entries and maps them to standardized categories based on the UK census
        style guide. It uses keyword matching against predefined lists of
        ethnicities, nationalities, and countries.

        The mapping logic relies on several assumptions and configurations:

        - It uses exact (case-insensitive) keyword matching, not fuzzy matching.
        - It can be configured to assume default ethnicities for certain
          nationalities (e.g., British -> White, Nigerian -> Black).
        - Explicit racial terms (e.g., "White", "Black") in an entry take
          precedence over national or country terms.

        Note:
            The keyword lists and mapping logic may contain ambiguities. Manual
            review of the output is recommended. The `outputNameString` parameter
            is currently unused within the function's logic.

        Args:
            dataFrame: The DataFrame containing the ethnicity data.
            outputNameString: A string to prefix an output filename (currently
                unused).
            ethnicityColumnString: The name of the column in `dataFrame` that
                contains the free-text ethnicity entries.

        Returns:
            A new DataFrame with an added 'census' column containing the
            mapped ethnicity categories.
        """
        assumeBritishWhite = True
        assumeEnglishWhite = True
        assumeEuropeanWhite = True
        assumeAfricanBlack = True
        assumeAsianAsian = True
        assumeSouthAmericanOther = True
        assumeNorthAmericanOther = True
        includeNationalitiesForCountries = True
        edgeCases = True

        targetList = dataFrame

        outputNameString + ".csv"

        targetList.columns

        targetColumnString = ethnicityColumnString



        len(targetList[targetColumnString].unique())

        len(targetList[targetColumnString])

        targetList[targetColumnString].nunique()

        racecodeEntries = targetList[targetColumnString].tolist()

        racecodeEntries = pd.DataFrame(racecodeEntries, columns=[targetColumnString])

        racecodeEntries[targetColumnString] = racecodeEntries[
            targetColumnString
        ].fillna("other_ethnic_group")

        df_testMap = dataFrame[["client_idcode", ethnicityColumnString]].copy()

        for col in dataFrame.columns:
            df_testMap[col] = dataFrame[col]

        df_testMap.insert(1, "census", "other_ethnic_group")

        # Set default value as other
        df_testMap["census"][0] = "other_ethnic_group"

        whiteList = [
            "English",
            "Welsh",
            "Scottish",
            "Northern",
            "Irish",
            "British",
            "Irish",
            "Gypsy",
            "Irish Traveller",
            "Any other White background",
        ]






        # Groups derived from https://www.ethnicity-facts-figures.service.gov.uk/style-guide/ethnic-groups

        blackList = [
            "black",
            "african",
            "caribbean",
            "black british",
            "black african",
            "black carribean",
        ]

        whiteList = [
            "white",
            "caucasian",
            "gypsy",
            "traveller",
            "other white",
            "white other",
        ]

        asianList = ["asian", "chinese", "pakistani", "bangladeshi", "indian"]

        otherList = ["arab", "not specified"]

        mixedList = [
            "mixed",
            "multiple",
            "biracial",
            "multiracial",
            "white and asian",
            "white and black",
            "white and hispanic",
            "black and white",
            "asian and white",
            "hispanic and white",
        ]

        allEthList = blackList + whiteList + asianList + otherList + mixedList

        africanCountries = [
            "algeria",
            "angola",
            "benin",
            "botswana",
            "burkina",
            "faso",
            "burundi",
            "cabo",
            "verde",
            "cameroon",
            "central african republic",
            "chad",
            "comoros",
            "congo,",
            "democratic",
            "republic of the congo",
            "republic of the cote d'ivoire",
            "djibouti",
            "egypt",
            "equatorial",
            "guinea",
            "eritrea",
            "eswatini",
            "ethiopia",
            "gabon",
            "gambia",
            "ghana",
            "guinea",
            "guinea-bissau",
            "kenya",
            "lesotho",
            "liberia",
            "libya",
            "madagascar",
            "malawi",
            "mali",
            "mauritania",
            "mauritius",
            "morocco",
            "mozambique",
            "namibia",
            "niger",
            "nigeria",
            "rwanda",
            "sao tome and principe",
            "senegal",
            "seychelles",
            "sierra",
            "leone",
            "somalia",
            "south",
            "africa",
            "south",
            "sudan",
            "sudan",
            "tanzania",
            "togo",
            "tunisia",
            "uganda",
            "zambia",
            "zimbabwe",
        ]

        asianCountries = [
            "afghanistan",
            "armenia",
            "azerbaijan",
            "bahrain",
            "bangladesh",
            "bhutan",
            "brunei",
            "cambodia",
            "china",
            "cyprus",
            "east",
            "timor",
            "egypt",
            "georgia",
            "india",
            "indonesia",
            "iran",
            "iraq",
            "israel",
            "japan",
            "jordan",
            "kazakhstan",
            "kuwait",
            "kyrgyzstan",
            "laos",
            "lebanon",
            "malaysia",
            "maldives",
            "mongolia",
            "myanmar",
            "nepal",
            "north",
            "korea",
            "oman",
            "pakistan",
            "palestine",
            "philippines",
            "qatar",
            "russia",
            "saudi arabia",
            "singapore",
            "south korea",
            "sri lanka",
            "syria",
            "taiwan",
            "tajikistan",
            "thailand",
            "turkey",
            "turkmenistan",
            "united arab emirates",
            "uzbekistan",
            "vietnam",
            "yemen",
        ]

        europeanCountries = [
            "albania",
            "andorra",
            "armenia",
            "austria",
            "azerbaijan",
            "belarus",
            "belgium",
            "bosnia and herzegovina",
            "bulgaria",
            "croatia",
            "cyprus",
            "czechia",
            "denmark",
            "estonia",
            "finland",
            "france",
            "georgia",
            "germany",
            "greece",
            "hungary",
            "iceland",
            "ireland",
            "italy",
            "kazakhstan",
            "kosovo",
            "latvia",
            "liechtenstein",
            "lithuania",
            "luxembourg",
            "malta",
            "moldova",
            "monaco",
            "montenegro",
            "netherlands",
            "north macedonia",
            "norway",
            "poland",
            "portugal",
            "romania",
            "russia",
            "san marino",
            "serbia",
            "slovakia",
            "slovenia",
            "spain",
            "sweden",
            "switzerland",
            "turkey",
            "ukraine",
            "united kingdom",
            "vatican city",
        ]

        northAmericanCountries = [
            "antigua and barbuda",
            "bahamas",
            "barbados",
            "belize",
            "canada",
            "costa rica",
            "cuba",
            "dominica",
            "dominican republic",
            "el salvador",
            "grenada",
            "guatemala",
            "haiti",
            "honduras",
            "jamaica",
            "mexico",
            "nicaragua",
            "panama",
            "saint kitts and nevis saint lucia",
            "saint vincent and the grenadines",
            "trinidad and tobago",
        ]

        southAmericanCountries = [
            "argentina",
            "bolivia",
            "brazil",
            "chile",
            "colombia",
            "ecuador",
            "guyana",
            "paraguay",
            "peru",
            "suriname",
            "uruguay",
            "venezuela",
        ]

        africanNationalities = [
            "Swazi",
            "algerian",
            "angolan",
            "beninese",
            "botswanan",
            "burkinese",
            "burundian",
            "cameroonian",
            "cape verdeans",
            "chadian",
            "congolese",
            "djiboutian",
            "egyptian",
            "eritrean",
            "ethiopian",
            "gabonese",
            "gambian",
            "ghanaian",
            "guinean",
            "kenyan",
            "krio people",
            "liberian",
            "libyan",
            "madagascan",
            "malagasy",
            "malawian",
            "malian",
            "mauritanian",
            "mauritian",
            "moroccan",
            "mozambican",
            "namibian",
            "nigerian",
            "nigerien",
            "rwandan",
            "senegalese",
            "somali",
            "sudanese",
            "tanzanian",
            "togolese",
            "tunisian",
            "ugandan",
            "zambian",
            "zimbabwean",
            "african",
        ]

        asianNationalities = [
            "afghan",
            "afghanistan",
            "armenian",
            "azerbaijani",
            "bahrain",
            "bahraini",
            "bangladesh",
            "bangladeshi",
            "bhutan",
            "bhutanese",
            "brunei",
            "burma",
            "burmese",
            "cambodia",
            "cambodian",
            "chinese",
            "filipino",
            "indian",
            "indonesian",
            "iranian",
            "iraqi",
            "japanese",
            "jordanian",
            "kazakh",
            "kuwaiti",
            "laotian",
            "lebanese",
            "malawian",
            "malaysian",
            "maldivian",
            "mongolian",
            "myanmar",
            "nepalese",
            "omani",
            "pakistani",
            "philippine",
            "qatari",
            "russian",
            "singaporean",
            "sri lankan",
            "syrian",
            "tadjik",
            "taiwanese",
            "tajik",
            "thai",
            "turkish",
            "turkmen",
            "turkoman",
            "uzbek",
            "vietnamese",
            "yemeni",
            "punjabi",
            "kurdish",
            "tamil",
            "kashmiri",
            "sinhala",
            "sinhalese",
        ]

        europeanNationalities = [
            "albanian",
            "andorran",
            "armenian",
            "australian",
            "austrian",
            "azerbaijani",
            "belarusan",
            "belarusian",
            "belgian",
            "bosnian",
            "brit",
            "british",
            "bulgarian",
            "croat",
            "croatian",
            "cypriot",
            "czech",
            "danish",
            "dutch",
            "english",
            "estonian",
            "finnish",
            "french",
            "georgian",
            "german",
            "greek",
            "holland",
            "hungarian",
            "icelandic",
            "irish",
            "italian",
            "latvian",
            "lithuanian",
            "maltese",
            "moldovan",
            "monacan",
            "montenegrin",
            "monégasque",
            "netherlands",
            "norwegian",
            "polish",
            "portuguese",
            "romanian",
            "scot",
            "scottish",
            "serb",
            "serbian",
            "slovak",
            "slovene",
            "slovenian",
            "spanish",
            "swedish",
            "swiss",
            "ukrainian",
            "welsh",
            "yugoslav" "ussr",
            "soviet",
            "cornish",
        ]



        if assumeBritishWhite:
            whiteList.append("british")

        if assumeEnglishWhite:
            whiteList.append("english")

        if assumeEuropeanWhite:
            whiteList = whiteList + europeanCountries

        if assumeAfricanBlack:
            blackList = blackList + africanCountries

        if assumeAsianAsian:
            asianList = asianList + asianCountries

        if assumeSouthAmericanOther:
            otherList = otherList + southAmericanCountries

        if assumeNorthAmericanOther:
            otherList = otherList + northAmericanCountries

        if includeNationalitiesForCountries:
            whiteList = whiteList + europeanNationalities

            blackList = blackList + africanNationalities

            asianList = asianList + asianNationalities

            otherList = otherList + southAmericanCountries + northAmericanCountries

        if edgeCases:

            extraWhite = [
                "australian",
                "american",
                "usa",
                "united states",
                "the united states of america",
                "canadian",
            ]
            whiteList = whiteList + extraWhite

        allEthList = blackList + whiteList + asianList + otherList + mixedList

        # print(len(racecodeEntries))

        # for i in tqdm(range(0, len(racecodeEntries))):
        for i in range(0, len(racecodeEntries)):

            entry = racecodeEntries[targetColumnString][i].lower()

            # print(entry)

            res = "other_ethnic_group"

            count = 0

            for synonym in whiteList:
                if synonym in entry and entry not in set(allEthList).difference(
                    set(whiteList)
                ):
                    count = count + 1
                    res = "white"
                    # print(entry, 'white')

            for synonym in asianList:
                if synonym in entry and entry not in set(allEthList).difference(
                    set(asianList)
                ):
                    count = count + 1
                    res = "asian_or_asian_british"

            for synonym in blackList:
                if synonym in entry and entry not in set(allEthList).difference(
                    set(blackList)
                ):
                    count = count + 1
                    res = "black_african_caribbean_or_black_british"

            for synonym in otherList:
                if synonym in entry and entry not in set(allEthList).difference(
                    set(otherList)
                ):
                    count = count + 1
                    res = "other_ethnic_group"
                    # print(entry, 'other_ethnic_group2')

            for synonym in mixedList:
                if synonym in entry and entry not in set(allEthList).difference(
                    set(mixedList)
                ):
                    count = count + 1
                    res = "mixed_or_multiple_ethnic_groups"

            # Explicit/specification:
            if "other" in entry and entry not in set(allEthList).difference(
                set(otherList)
            ):
                res = "other_ethnic_group"
                # print(entry, 'other_ethnic_group')

            if "black" in entry and entry not in set(allEthList).difference(
                set(blackList)
            ):
                res = "black_african_caribbean_or_black_british"

            if "white" in entry and entry not in set(allEthList).difference(
                set(whiteList)
            ):
                res = "white"

            if "asian" in entry and entry not in set(allEthList).difference(
                set(asianList)
            ):
                res = "asian_or_asian_british"

            if "mix" in entry and entry not in set(allEthList).difference(
                set(mixedList)
            ):
                res = "mixed_or_multiple_ethnic_groups"

            if count > 15:
                # print("Mixed found:")
                # print(entry)
                # print(entry)
                # res = 'Mixed or Multiple ethnic groups'
                pass

            # print("Returning ", res)
            df_testMap["census"][i] = res

        return df_testMap
