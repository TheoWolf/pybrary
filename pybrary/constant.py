import re

session_headers = [
    "^session-id$",
    "^session-name$",
    "^session-date$",
    "^pilot-pilot$",
    "^session-release$",
    "^exclusion[1-9]?-reason$",
    "^task[1-9]?-name$",
    "^context-country$",
    "^context-state$",
    "^context-language$",
    "^participant[1-9]?-ID$",
    "^context-setting$",
    "^group-name$"
]


participant_headers = [
    "^session-id$"
    "^participant[1-9]?-ID$",
    "^participant[1-9]?-birthdate$",
    "^participant[1-9]?-gender$",
    "^participant[1-9]?-race$",
    "^participant[1-9]?-ethnicity$",
    "^participant[1-9]?-language$",
    "^participant[1-9]?-pregnancy term$",
    "^participant[1-9]?-disability$"
]

session_headers_format = {
    "key": re.compile("^session-id$"),
    "name": re.compile("^session-name$"),
    "date": re.compile("^session-date$"),
    "pilot": re.compile("^pilot-pilot$"),
    "release": re.compile("^session-release$"),
    "exclusion": re.compile("^exclusion[1-9]?-reason$"),  # exclusion1-reason,exclusion2-reason
    "tasks": re.compile("^task[1-9]?-name$"),  # task1-name,task2-name,task3-name
    "country": re.compile("^context-country$"),
    "state": re.compile("^context-state$"),
    "language": re.compile("^context-language$"),
    "participantID": re.compile("^participant[1-9]?-ID$"),
    "setting": re.compile("^context-setting$"),
    "group": re.compile("^group-name$")
}

participant_headers_format = {
    "participantID": re.compile("^participant[1-9]?-ID$"),
    "birthdate": re.compile("^participant[1-9]?-birthdate$"),
    "gender": re.compile("^participant[1-9]?-gender$"),
    "race": re.compile("^participant[1-9]?-race$"),
    "ethnicity": re.compile("^participant[1-9]?-ethnicity$"),
    "language": re.compile("^participant[1-9]?-language$"),
    "pregnancy term": re.compile("^participant[1-9]?-pregnancy term$"),
    "disability": re.compile("^participant[1-9]?-disability$"),
}

db_formats = {
    "2": "csv",
    "4": "rtf",
    "5": "png",
    "6": "pdf",
    "7": "doc",
    "8": "odf",
    "9": "docx",
    "10": "xls",
    "11": "ods",
    "12": "xlsx",
    '13': "ppt",
    "14": "odp",
    "15": "pptx",
    "16": "opf",
    "18": "webm",
    "20": "mov",
    "-800": "mp4",
    "22": "avi",
    "23": "sav",
    "24": "wav",
    "19": "mpeg",
    "26": "chat",
    "-700": "jpeg",
    "21": "mts",
    "-600": "mp3",
    "27": "aac",
    "28": "wma",
    "25": "wmv",
    "29": "its",
    "30": "dv",
    "1": "txt",
    "31": "etf"
}