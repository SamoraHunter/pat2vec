import re
import pandas as pd


def stringlist2searchlist(string_list, output_name):
    list_string = string_list.replace("\n", '" OR "')
    textfile = open(output_name + ".txt", "w")
    textfile.write(f'"{list_string}"')
    textfile.close()
    print("List processed!")


def pylist2searchlist(list_name, output_name):
    test_str = '" OR "'.join(list_name)
    textfile = open(output_name + ".txt", "w")
    textfile.write(f'"{test_str}"')
    textfile.close()


def stringlist2pylist(string_list, var_name):
    globals()[var_name] = string_list.replace("\n", ",").split(",")
    print("List generated!")


def date_cleaner(df, cols, date_format):
    for col in cols:
        df[col] = pd.to_datetime(df[col], utc=True).dt.strftime(date_format)
    print("dates formatted!")


def bulk_str_findall(target_colname_regex_pairs, source_colname, df_name):
    for key, value in target_colname_regex_pairs.items():
        df_name[key] = (
            df_name[source_colname].str.lower().str.findall(value).str.join(",\n")
        )


def bulk_str_extract(target_colname_regex_pairs, source_colname, df_name, expand):
    for key, value in target_colname_regex_pairs.items():
        df_name[key] = (
            df_name[source_colname]
            .str.lower()
            .str.extract(pat=value, expand=expand, flags=re.IGNORECASE)
        )


def without_keys(d, keys):
    return {k: v for k, v in d.items() if k not in keys}


def bulk_str_extract_round_robin(target_dict, df_name, source_colname, expand):
    for key, value in target_dict.items():
        remaining_dict = without_keys(target_dict, key)
        remaining_strings = "|".join(list(remaining_dict.values()))
        df_name[key] = (
            df_name[source_colname]
            .str.lower()
            .str.extract(
                pat=f"{value}(.*?)({remaining_strings})",
                expand=expand,
                flags=re.IGNORECASE,
            )[0]
        )
