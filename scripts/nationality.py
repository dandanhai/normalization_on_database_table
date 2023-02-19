import pymysql
import pandas as pd
import configparser

config = configparser.ConfigParser()
config.read("../config.ini")

USERNAME = config.get("database", "USERNAME")
HOST_IP = config.get("database", "HOST_IP")
PASSWORD = config.get("database", "PASSWORD")
DATABASE = config.get("database", "DATABASE")

NATIONALITY_DICT = {
    "SG": ["SG", "SGP", "Singaporean", "Singaporean ", "Singapore", "Singapore ", "SINGAPOREAN", "singaporean",
           "Singpaorean", "Sg", "singapore ", "singapore", "SPR", "Sinagapore", "SINGAPOREAN ", "SINGAPORE", "Bugis",
           "SINGAPORE CITIZEN", "Singapore Citizen", "Singaporeans ", "singopre", "singaporean "],
    "MY": ["MY", "Malaysia", "Malaysian", "MYS", "MALAYSIA", "malaysia", "MALAYSIAN", "Malaysia ", "Malaysian ",
           "malaysian", "Malay", "Melayu", "ALAYSIA ", "malaysia ", "MALAYSIAN ", "malay", "MALAY", "My", "MELAYU",
           "Bumiputera", "马来西亚", "Malaysians", "Malayaia", " Malaysian", "melayu", "Bumiputra", "Malysia", "Selangor",
           "Malaysia n", "Malaysion", "Sabah", "malaysian ", "BUMIPUTERA", "Malay ", "Melaka", "Maaysia", "Warganegara",
           "warganegara", "WARGANEGARA", "Warganegara ", "Warganera", "Warganegara malaysia", "Maaysian", "Makaysia",
           "Warga negara", "Malayasian", "Malysian", "Malayu", "melaysia", "Malaysians ", "Dusun", "Malaysian Chinese",
           "warga negara", "Sunagai petani", "Seremban", "Sabah , Malaysia", "Penang", "Bumi putra", "malayasia", "my",
           "malysian", "malayysia", "malaysian/thai", " Malaysia", "malaysiam", "malaysa", "malayaia", " malaysia",
           "malalysian", "perak", "sabah", "馬來西亞", "warganagara", " Malay", "sarawak", "bumiputera", "Warnegara",
           "Wilayah Persekutuan, Kuala Lumpur", "Warganegara Malaysia", "Warga negara Malaysia", "Malaysiaĺ", "Kl",
           "Kuala lumpur", "Kuala Lumpur", "Kota kinabalu sabah", "KUCHING", "KL", "BUMIPUTRA", "Kuala lumpur ",
           "Kuching", "Malaysia’s ", "Malysian ", "Mallaysia", "Malaysis", "Malaysin ", "Malaysin", "Bumiputera Sarawak",
           "Malaysian chinese", "Malaysian Citizen", "Malaysian  ", "Malaysiam", "Malaysiaan", "Malaysiaa", "Maysia ",
           "Melayasia", 'Mqlaysia', "NALAYSIA", "Malaysi", "MALAYSUA", "MMALAYSIA", "MAlAYSIAN", "Malaysai", "MALAYSIA N",
           "MALAYSI", "MALAYSAIA", "MAAYSIA", "MalaysIa", "Malayian", "Malayia", "Malaydian", "Malaydia", "Malayasia",
           "Malayaian", "Malasyia ", "Malasian", "Malasia", "Malalysian", "Mala", "MAlaysian"],
    "PH": ["PH", "PHL", "Filipino", "filipino", "Filipino ", "Philippines", "Philippines ", "Philippine", "PHILIPPINES",
           "PHILIPINES", "Flipino", "FILIPINO"],
    "ID": ["ID", "IDN", "Indonesian", "Indonesia", "indonesia", " Indonesian", "Indonesian ", "Indonasian "],
    "IN": ["IN", "IND", "Indian ", "Indian"],
    "HK": ["HK", "Hong Kong", "Hong kong", "香港", "Hk", "中國香港", "hong kong", "Hongkong", "HKSAR", "Hong kong ", "hk",
           "Hong Kong ", "hongkong", "HONG KONG", "Hong Kong SAR", "Hksar", "Hk ", "Hong Kong, China", "Hongkong ",
           "china hong kong", "hong kong sar", "hong king", "hksar", "  Hong kong", "中國 香港", "\\u3000Hong Kong ",
           "Hkhk", "Hong kwok", "Hong konh", "Hong kong, China ", "Hong Kong china", "Hong Kong China", "Hkg", "Hkc",
           "Honk kong, china", "HkSAR", "Hing Kong", "HONG KONG SAR", "HKG", "H.K.", "Chinese hong Kong", "CHN(HK)",
           "China, Hong Kong", "China Hong Kong", "China Hk"],
    "CN": ["CN", "CHN", "Chinese", "中國", "China", "Chinese ", "china", "chinese", "Cina", "cina", "CHINA", "CHINESE",
           "China ", "华人", "中籍", " China", "west chinise"],
    "CH": ["CH", "CHE", "Swiss"],
    "TH": ["TH", "Thailand", "Thai", "ไทย", "THA", "thai", "Thailand ", "Siamese", " Thai", "  Thai", "thailand",
           "Thi", "Thailand\\u200b"],
    "VN": ["VN", "VNM", "Vietnam", "Vietnam ", "Vietnamese", "Vietnamese "],
    "KR": ["KR", "KOR", "Republic of korea", "South Korea", "Korean"],
    "TW": ["TW", "TWN", "Taiwan", "Taiwanese ", "Taiwan "],
    "GB": ["GB", "GBR", "UK", "British", "Untied kingdom ", "United Kingdom of Great Britain and Northern Ireland",
           "british", "British "],
    "AU": ["AU", "AUS", "Australian ", "Australian"],
    "US": ["US", "USA", "American", "United States of America", "United States", "American "],
    "JP": ["JP", "JPN", "Japan", "日本", "Japanese"],
    "MM": ["MM", "MMR", "Myanmar ", "Myanmar", "burmese", "MYANMAR "],
    "FR": ["FR", "FRA", "RF", "french", "French"],
    "LK": ["LK", "LKA", "Singhalese"],
    "CA": ["CA", "CAN", "Canadian", "Canada"],
    "BD": ["BD", "BGD", "Bangladeshi ", "Bangladeshi", "Bangladesh "],
    "PK": ["PK", "PAK"],
    "NP": ["NP", "NPL"],
    "BG": ["BG", "BU"],
    "NZ": ["NZ", "NZL", "New Zealand"],
    "IT": ["IT", "ITA", "Italiana", "Italian"],
    "ZA": ["ZA", "ZAF", "South African "],
    "GG": ["GG"],
    "BN": ["BN", "BRN"],
    "BR": ["BR", "BRA"],
    "MX": ["MX", "MEX"],
    "MV": ["MV", "MDV"],
    "BE": ["BE", "BEL"],
    "ES": ["ES", "ESP"],
    "SS": ["SS"],
    "IE": ["IE", "IRL"],
    "IL": ["IL"],
    "UY": ["UR"],
    "PT": ["PT"],
    "MN": ["MN", "MNG"],
    "TR": ["TR", "TUR"],
    "CO": ["CO", "Colombian", "COL"],
    "AO": ["AO"],
    "AR": ["AR"],
    "AS": ["ASM"],
    "AT": ["AT", "AUT", "Austrian "],
    "KZ": ["KZ", "KAZ"],
    "EG": ["EG", "EGY"],
    "RO": ["RO", "Romanian"],
    "RU": ["RU", "RUS"],
    "SE": ["SE"],
    "MU": ["MU"],
    "AD": ["AD"],
    "IR": ["IR", "Iran"],
    "AI": ["AI"],
    "DE": ["DE", "DEU", "German"],
    "DK": ["DK"],
    "PL": ["PL", "POL"],
    "PE": ["PE", "PER"],
    "VE": ["VE"],
    "MA": ["MA"],
    "AQ": ["AQ"],
    "CR": ["CR"],
    "KP": ["PRK"],
    "AZ": ["AZ", "AZE"],
    "AM": ["AM"],
    "KH": ["KH", "KHM", "Cambodian"],
    "CL": ["CL"],
    "GR": ["GR", "GRC"],
    "EC": ["EC"],
    "NL": ["NL", "NLD"],
    "NG": ["NG"],
    "TT": ["TT", "TTO"],
    "CY": ["CY"],
    "UA": ["UKR"],
    "VU": ["VU"],
    "LB": ["LB", "LEBANESE"],
    "AL": ["AL"],
    "NO": ["NO", "no"],
    "SK": ["SK", "SVK"],
    "FI": ["FI", "FIN", "Finnish"],
    "RS": ["RS"],
    "SA": ["SA", "SAU"],
    "HU": ["HU", "HUN"],
    "DZ": ["DZ"],
    "TD": ["TD"],
    "TN": ["TN", "TUN"],
    "SC": ["SC"],
    "PG": ["PNG"],
    "UZ": ["UZ"],
    "UG": ["UG"],
    "TZ": ["TZ"],
    "YE": ["YEM"],
    "BF": ["BF"],
    "BL": ["BL"],
    "HR": ["HRV"],
    "BO": ["BO"],
    "CD": ["CD"],
    "GN": ["GN"],
    "GH": ["GH"],
    "GF": ["GF"],
    "BT": ["BT"],
    "BZ": ["BZ"],
    "EE": ["EST"],
    "LA": ["LA"],
    "ML": ["Mali ", "Mali"],
    "NA": ["NAM"],
    "MP": ["MNP"],
    "MG": ["MG"],
    "MO": ["MO"],
    "MR": ["MRT"],
    "MT": ["MT"]
}

db = pymysql.connect(
    host=HOST_IP,
    user=USERNAME,
    password=PASSWORD,
    database=DATABASE
)
cursor = db.cursor()

cursor.execute("SELECT location_id, nationality FROM customers;")
data = cursor.fetchall()
data = pd.DataFrame(data)

nationality_series = data[1]
nationality_series = nationality_series.astype("category")
nationality_distribution = nationality_series.value_counts()
nationality_distribution.to_csv("../display/nationality.csv")

nationality_distribution = dict(nationality_distribution)

clean_nationality_list = []
key_list = list(NATIONALITY_DICT.keys())
for i in range(len(data[1])):
    flag = True
    if data[1][i] in key_list:
        clean_nationality_list.append(data[1][i])
        flag = False
    else:
        for index in range(len(key_list)):
            if data[1][i] in NATIONALITY_DICT[key_list[index]]:
                clean_nationality_list.append(key_list[index])
                flag = False
                break
    if flag:
        clean_nationality_list.append(data[0][i])
            
print(len(clean_nationality_list))
data["clean nationality abbreviation"] = clean_nationality_list
data.to_csv("../display/nationality_clean.csv")
