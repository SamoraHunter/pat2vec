import pandas as pd

"""
The EthnicityAbstractor module processes free-text ethnicity entries and maps them to standardized UK census categories.

This module provides tools for converting diverse ethnic self-identification text into compliant UK census format.
It is designed to process a column of free-text entries in a CSV file under an "ethnicity" field, matching
these with corresponding categories from the UK census categories as outlined in the style guide provided by
https://www.ethnicity-facts-figures.service.gov.uk/style-guide/ethnic-groups.

Features:
    - Exact keyword matching (case-insensitive)
    - Configurable default ethnicities for specific nationalities
    - Explicit racial terms take precedence over national/country terms
    - Predefined lists of ethnic groups, countries, and nationalities

Note:
    The categorization lists and output may contain errors and ambiguities.
    Manual review of outputs is strongly recommended.

Example usage:
    >>> abstractor = EthnicityAbstractor()
    >>> df_result = abstractor.abstractEthenticity(df, "output", "ethnicity_col")
"""


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

        targetColumnString = ethnicityColumnString

        racecodeEntries = targetList[targetColumnString].tolist()

        racecodeEntries = pd.DataFrame(racecodeEntries, columns=[targetColumnString])

        racecodeEntries[targetColumnString] = racecodeEntries[
            targetColumnString
        ].fillna("other_ethnic_group")

        df_testMap = dataFrame.copy()

        df_testMap.insert(1, "census", "other_ethnic_group")

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
            "English",
            "Welsh",
            "Scottish",
            "Northern",
            "Irish",
            "British",
            "Gypsy",
            "Irish Traveller",
            "Any other White background",
            "white",
            "caucasian",
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
            "yugoslav",
            "ussr",
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

        # Pre-calculate exclusion sets to improve performance
        all_eth_set = set(allEthList)
        white_diff = all_eth_set.difference(set(whiteList))
        asian_diff = all_eth_set.difference(set(asianList))
        black_diff = all_eth_set.difference(set(blackList))
        other_diff = all_eth_set.difference(set(otherList))
        mixed_diff = all_eth_set.difference(set(mixedList))

        for i in range(0, len(racecodeEntries)):
            entry = racecodeEntries[targetColumnString][i].lower()
            res = "other_ethnic_group"
            count = 0

            for synonym in whiteList:
                if synonym in entry and entry not in white_diff:
                    count = count + 1
                    res = "white"

            for synonym in asianList:
                if synonym in entry and entry not in asian_diff:
                    count = count + 1
                    res = "asian_or_asian_british"

            for synonym in blackList:
                if synonym in entry and entry not in black_diff:
                    count = count + 1
                    res = "black_african_caribbean_or_black_british"

            for synonym in otherList:
                if synonym in entry and entry not in other_diff:
                    count = count + 1
                    res = "other_ethnic_group"

            for synonym in mixedList:
                if synonym in entry and entry not in mixed_diff:
                    count = count + 1
                    res = "mixed_or_multiple_ethnic_groups"

            # Explicit/specification:
            if "other" in entry and entry not in other_diff:
                res = "other_ethnic_group"

            if "black" in entry and entry not in black_diff:
                res = "black_african_caribbean_or_black_british"

            if "white" in entry and entry not in white_diff:
                res = "white"

            if "asian" in entry and entry not in asian_diff:
                res = "asian_or_asian_british"

            if "mix" in entry and entry not in mixed_diff:
                res = "mixed_or_multiple_ethnic_groups"

            if count > 15:
                # print("Mixed found:")
                # print(entry)
                # print(entry)
                # res = 'Mixed or Multiple ethnic groups'
                pass

            # print("Returning ", res)
            df_testMap.at[i, "census"] = res

        return df_testMap
