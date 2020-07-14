import sys
import os
import pandas as pd
from pybrary import Pybrary
from constant import participant_headers
from constant import session_headers_format
from constant import participant_headers_format
from constant import session_headers
from constant import db_formats


class Curation:
    __api = None

    def __init__(self, username, password, superuser=False):
        try:
            self.__api = Pybrary.getInstance(username, password, superuser)
        except AttributeError as e:
            sys.exit(e)

    def parseDF(self, df):
        id = ["", "1", "2", "3", "4", "4", "5", "6",
              "7", "8", "9"]
        participants = []
        for i in id:
            # Build an array of participants fields regex
            participant_regex = [
                "^participant"+i+"-ID$",
                "^participant"+i+"-birthdate$",
                "^participant"+i+"-gender$",
                "^participant"+i+"-race$",
                "^participant"+i+"-ethnicity$",
                "^participant"+i+"-language$",
                "^participant"+i+"-pregnancy term$",
                "^participant"+i+"-disability$"
            ]
            # Single participant
            participant = []
            for regex in participant_regex:
                filter = df.filter(regex=(regex))
                if not filter.empty:
                    # We need to rename columns to easily concatenate
                    filter = self.renameHeaders(filter, participant_headers_format)
                    participant.append(filter)

            if len(participant) > 0:
                #  We use the session id as a common denominator between participants and sessions
                participant.append(df['session-id'])
                # concatenate fields found for participant i
                participant = pd.concat(participant, axis=1)
                #  Drop all rows that does not contain a participant ID
                # TODO DO NOT REMOVE PARTICIPANT WITH NO PARTICIPANT ID
                #  YOU NEED TO PARSE THE SESSION FILES AND REMOVE ROWS WITH NO FILES AND PARTICIPANT ID
                participant = participant.dropna(subset=['participantID'], how='all')
                # Append the session
                participants.append(participant)

        participants = pd.concat(participants, ignore_index=True)
        participants.to_csv('part.csv')
        sessions = []
        for regex in session_headers:
            filter = df.filter(regex=(regex))
            if not filter.empty:
                sessions.append(filter)

        sessions = pd.concat(sessions, axis=1)
        sessions.to_csv('sess.csv')
        for index, session in sessions.iterrows():
            participants_in_session = participants.loc[participants['session-id'] == session['session-id']]
            if not participants_in_session.empty:
                print('found ' + str(len(participants_in_session)) + ' participant in session ' + str(session['session-id']))
                for i, participant in participants_in_session.iterrows():
                    participant_id = participant[0]
                    sessions.loc[(session['session-id'] == participants['session-id'])
                                & (session['participant'+str(i+1)+'-ID'] == participants['participantID'])] = participant_id

        sessions.to_csv('sess_2.csv')


        # Remove particpants headers
        # Replace partipant ID by index in participant DF (find both session-id and participant id before replacing)
        # Save the file with extra particpants
        # Display a warning that extra participants need to be uploaded manually
        # Replace particpantID values by row index
        # Remove extra participant
        # Rename session header
        return None, None

    def parseCSV(self, df):

        def filterDataFrame(dataframe, regex_array, axis=1):
            result = []
            for regex in regex_array:
                filter = dataframe.filter(regex=(regex))
                if not filter.empty:
                    result.append(filter)

            return pd.concat(result, axis=axis)

        # handle more than one participant and empty participant
        # remove empty lines
        participants = filterDataFrame(df, participant_headers)
        sessions = filterDataFrame(df, session_headers)

        return participants, sessions

    def getCSV(self, source, dir=os.getcwd()):
        return self.__api.get_csv(source, dir)

    def getAssets(self, df, source, target, directory='/nyu/stage/reda/'):
        """
        Retrieve assets from Databrary and append a file name, file path and file clips in for each asset in the session
        :param df: Dataframe
        :param source: The original volume
        :param target: The new volume
        :param directory: path to the files on the server
        :return: A Dtaframe with session assets
        """
        folder = directory + str(target)
        for index, row in df.iterrows():
            try:
                session = row['session-id']
                print('Fetching session ' + str(session))
                assets = self.__api.get_session_assets(source, session)
                #     Need to update session df with the right paths
                for i, asset in enumerate(assets):
                    # Need to add clip_in here
                    df.at[index,'file_' + str(i + 1)] = folder + '/' + str(target) + str(session) + '/' \
                                               + asset['name'] + '.' + self.getFormat(asset['format'])
                    df.at[index,'fname_' + str(i + 1)] = asset['name']

                    if self.isMedia(asset['format']):
                        df.at[index,'clip_in_' + str(i + 1)] = ''

            except AttributeError as e:
                raise

        return df

    def getFormat(self, format_id):
        return db_formats.get(str(format_id), '')

    def isMedia(self, format):
        """
        Check if a file format is a Media (Video, Audio)
        :param format: file format
        :return: return true if format is a Video or Audio
        """
        if self.getFormat(format) == 'mp4' \
                or self.getFormat(format) == 'mp3' \
                or self.getFormat(format) == 'mpeg' \
                or self.getFormat(format) == 'avi' \
                or self.getFormat(format) == 'mov':
            return True
        return False

    def mergeTasks(self, df):
        tasks = df.filter(regex=("task[1-9]?-name"))
        if not tasks.empty:
            df['tasks'] = df[tasks].agg(';'.join, axis=1)
            df = df[df.columns.drop(list(tasks))]

        return df

    def reformatDate(self, df):
        pass

    def renameHeaders(self, df, header_regex):
        columns = {}
        for col in df.columns:
            for key, rx in header_regex.items():
                match = rx.search(col)
                if match:
                    columns.update({col: key})

        result = df.rename(columns=columns, errors="raise")
        return result

    def generateSQL(self, source, target, dir=os.getcwd()):
        """
        Generate Databrary DB query that need to be run before the ingest, it will copy volume's assets in the
        a staging folder.
        :param source: Original Volume ID
        :param target: New Volume ID
        :param dir: folder path
        :return:
        """
        query = "COPY (select 'mkdir -p /nyu/stage/reda/' || '" + str(target) + \
                "' || '/' || '" + str(target) + \
                "' || sa.container || ' && ' || E'cp \"/nyu/store/' || substr(cast(sha1 as varchar(80)), 3, 2) || '/' || " \
                "right(cast(sha1 as varchar(80)), -4) || '" "/nyu/stage/reda/' || '" \
                + str(target) + "' || '/' || '" + str(target) + \
                "' || container || '/' || CASE WHEN a.name LIKE '%.___' IS FALSE THEN a.name || '.' || f.extension[1] ELSE a.name END || E'\"' " \
                "from slot_asset sa inner join asset a on sa.asset = a.id inner join format f on a.format = f.id where a.volume = " \
                + str(source) + ") TO '/tmp/volume_" + str(target) + ".sh';"
        file = open(os.path.join(dir, 'query_'+str(target)+'.sql'), "w+")
        file.write(query)
        file.close()


    def prepareCSV(self, source, target):

        #  download csv
        csv_path = self.getCSV(source)

        df = pd.read_csv(csv_path)
        # df_assets = self.getAssets(df, source, target)
        # participants, sessions = self.parseDF(df)

        #  parse CSV
        # TODO(Reda): Fix missing Participant ID in session
        # TODO(Reda): Fix multiple Participant ID per session in both sessions and participants
        # participants, sessions = self.parseCSV(df)
        #  get assets for the sessions dataframe
        sessions = self.getAssets(df, source, target)
        #  Merge all tasks
        # sessions = self.mergeTasks(sessions)
        #  reformat date
        #  Rename headers
        # sessions = self.renameHeaders(sessions, session_headers_format)
        # participants = self.renameHeaders(participants, participant_headers_format)
        #  Write the sessions and participants
        sessions.to_csv('sessions_' + str(target) + '.csv')
        # self.generateSQL(source, target)
        # participants.to_csv('participants_' + str(target) + '.csv')
        return
