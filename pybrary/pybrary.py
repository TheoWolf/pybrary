import re
import os
import requests

from urllib.parse import urljoin

class Pybrary:
    __base_api_url = 'https://nyu.databrary.org/api/'
    __base_url = 'https://nyu.databrary.org/'
    __username = None
    __password = None
    __supersuer = None
    __session = None
    __instance = None

    @staticmethod
    def getInstance(username=None, password=None, superuser=False):
        if Pybrary.__instance is None:
            Pybrary(username, password, superuser)
        return Pybrary.__instance

    def __init__(self, username, password, superuser=False):
        if Pybrary.__instance is not None:
            raise Exception(
                'You are already logged in as {}, call Pybrary.getInstance().'.format(self.__username))
        else:
            self.__username = username
            self.__password = password
            self.__supersuer = superuser
            try:
                self.__session = self.__login(username, password, superuser)
            except AttributeError as e:
                raise
            Pybrary.__instance = self

    def __login(self, username, password, superuser):
        """
        Login to Databrary
        :param username: a valid user name (email)
        :param password: Databrary password
        :return: a session
        """
        session = requests.Session()
        url = urljoin(self.__base_api_url, 'user/login')
        credentials = {
            "email": username,
            "password": password,
            "superuser": superuser
        }

        response = session.post(url=url, json=credentials)
        if response.status_code == 200:
            response_json = response.json()
            if 'csverf' in response_json:
                session.headers.update({
                    "x-csverf": response_json['csverf']
                })

        else:
            raise AttributeError('Login failed, please check your username and password')

        return session

    def logout(self):
        """
        Disconnect from Databrary
        :return:
        """
        url = urljoin(self.__base_api_url, 'user/logout')
        response = self.__session.post(url=url)
        if response.status_code == 200:
            Pybrary.__instance = None
            __username = None
            __password = None
            __supersuer = None
            del self.__session
        else:
            raise AttributeError('Login failed, please check your username and password')

    def get_csv(self, volume_id, target_dir):
        """
        Download a CSV file from a Databrary volume, read access to the volume is required.
        :param volume_id: Databrary volume id
        :param target_dir: CSV file directory target
        :return: Path to the CSV file
        """

        def get_filename_from_cd(cd):
            """
            Get filename from content-disposition
            """
            if not cd:
                return None
            fname = re.findall('filename="(.+)"', cd)
            if len(fname) == 0:
                return None
            return fname[0]

        url = urljoin(self.__base_url, 'volume/' + str(volume_id) + '/csv')

        response = self.__session.get(url, allow_redirects=True)
        if response.status_code == 200:
            file_name = get_filename_from_cd(response.headers.get('content-disposition'))
            file_path = os.path.join(target_dir, file_name)
            open(file_path, 'wb').write(response.content)
            return file_path
        else:
            raise AttributeError('Cannot download CSV file from volume %d', volume_id)

    def get_session_records(self, volume_id, session_id):
        payload = {'records': 1}
        url = urljoin(self.__base_api_url, 'volume/' + str(volume_id) + '/slot/' + str(session_id))

        response = self.__session.get(url=url, params=payload)
        if response.status_code == 200:
            return response.json()['records']
        else:
            raise AttributeError('Cannot retrieve records list from session %d in volume %d', session_id, volume_id)

    def get_session_participants(self, volume_id, session_id):
        records = self.get_session_records(volume_id, session_id)
        participants_list = [record for record in records if record.get("record", {}).get("category") == 1]
        return participants_list

    def get_session_assets(self, volume_id, session_id):
        """
        Get volume's asset list
        :param volume_id: Databrary volume id
        :param session_id: Databrary session id
        :return: a list of session ids in JSON format
        """
        payload = {'assets': 1}
        url = urljoin(self.__base_api_url, 'volume/' + str(volume_id) + '/slot/' + str(session_id))

        response = self.__session.get(url=url, params=payload)
        if response.status_code == 200:
            return response.json()['assets']
        else:
            raise AttributeError('Cannot retrieve asset list from session %d in volume %d', session_id, volume_id)

    def get_sessions(self, volume_id):
        """
        Get a list of containers(session) from a Databrary volume
        :param volume_id: Databrary volume id
        :return: a list of session ids in JSON format
        """
        payload = {'containers': 1}
        url = urljoin(self.__base_api_url, 'volume/' + str(volume_id))

        response = self.__session.get(url=url, params=payload)
        if response.status_code == 200:
            return response.json()['containers']
        else:
            raise AttributeError(
                'Cannot retrieve sessions list from volume %d', volume_id)

    def get_volume_assets(self, volume_id):
        sessions = []
        for session in self.get_sessions(volume_id):
            session.update({"assets": self.get_session_assets(volume_id, session['id'])})
            sessions.append(session)
        return sessions

    def get_file_info(self, asset_id):
        url = urljoin(self.__base_api_url, 'asset/' + str(asset_id))

        response = self.__session.get(url=url)
        if response.status_code == 200:
            return response.json()
        else:
            raise AttributeError('Cannot retrieve asset %d info', asset_id)

    def post_file_name(self, asset_id, asset_name):
        payload = {'name': str(asset_name)}
        url = urljoin(self.__base_api_url, 'asset/' + str(asset_id))

        response = self.__session.post(url=url, json=payload)
        if response.status_code == 200:
            return response.json()
        else:
            raise AttributeError('Cannot change asset name to %s', str(asset_name))

    def post_file_permission(self, asset_id, permission):
        payload={
            'name': self.get_file_info(asset_id)['name'],
            'classification': permission
        }
        url=urljoin(self.__base_api_url, 'asset/' + str(asset_id))
        response=self.__session.post(url=url, json=payload)
        if response.status_code == 200:
            return response.json()
        else:
            raise AttributeError('Cannot change asset permission to %s', str(permission))

    def upload_asset(self, volume_id, session_id, file_path):
        """
        Upload OPF files to a Databrary session
        IMPORTANT: This method doesn't work with asset bigger than 1.04 MB
        :param volume_id:
        :param session_id:
        :param file_path:
        :return:
        """

        def create_asset(volume, session, filepath, token):
            payload = {
                'container': session,
                'name': utils.getFileName(filepath),
                'upload': token
            }
            url = urljoin(self.__base_api_url, 'volume/' + str(volume) + '/asset')

            response = self.__session.post(url=url, json=payload)
            if response.status_code == 200:
                return response.json()
            else:
                raise AttributeError('Cannot create asset om session %d volume %d', session, volume)

        def start_upload(volume, filepath):
            payload = {
                'filename': utils.getFileName(filepath),
                'size': utils.getFileSize(filepath)
            }
            url = urljoin(self.__base_api_url, 'volume/' + str(volume) + '/upload')

            response = self.__session.post(url=url, json=payload)
            if response.status_code == 200:
                return response.content
            else:
                raise AttributeError('Cannot get upload token for volume %d', volume)

        def upload_asset(volume, filepath, token):
            __fileChunckSize = 1048576
            __fileSize = utils.getFileSize(filepath)
            if __fileSize > __fileChunckSize:
                raise AttributeError('File size must be < than %d', __fileChunckSize)

            payload = {
                'flowChunkNumber': 1,
                'flowChunkSize': __fileChunckSize,
                'flowCurrentChunkSize': __fileSize,
                'flowTotalSize': __fileSize,
                'flowIdentifier': token,
                'flowFilename': utils.getFileName(filepath),
                'flowRelativePath': filepath,
                'flowTotalChunks': 1
            }
            url = urljoin(self.__base_api_url, 'upload')

            response = self.__session.get(url=url, params=payload)
            if response.status_code >= 400:
                raise AttributeError('Cannot upload file %s to volume %d', filepath, volume)

        try:
            upload_token = start_upload(volume_id, file_path)
            upload_asset(volume_id, file_path, upload_token)
            result = create_asset(volume_id, session_id, file_path, upload_token)
            return result
        except AttributeError as e:
            raise
