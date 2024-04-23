from office365.runtime.auth.authentication_context import AuthenticationContext
from office365.runtime.auth.user_credential import UserCredential
from office365.sharepoint.client_context import ClientContext
from office365.sharepoint.files.file import File 
from office365.sharepoint.files.creation_information import FileCreationInformation
import os
import pandas as pd

class SharepointFunctions():
    def __init__(self,username,password,sharepoint_url):
        #connect to sharepoint
        self.list = None
        self.ctx = self.connect_sharepoint_using_user(username,password,sharepoint_url)
        
    def connect_sharepoint_using_user(self,username,password,sharepoint_url):
        # Get sharepoint credentials
        #sharepoint_url = 'https://{your-tenant-prefix}.sharepoint.com'

        # Initialize the client credentials
        user_credentials = UserCredential(username, password)

        # create client context object
        ctx = ClientContext(sharepoint_url).with_credentials(user_credentials)

        web = ctx.web
        ctx.load(web)
        ctx.execute_query()

        print('Connected to SharePoint: ',web.properties['Title'])

        return ctx
    
    def get_sharepoint_connection(self):
        return self.ctx

###########  File related methods    ##################
    
    def get_folderlist_sharepoint(self, folder_url):
        '''
        return list of file_objects.
        example get names:
        for folder in folders:
            print(folder.properties["Name"])
        '''
        try:
            # file_url is the sharepoint url from which you need the list of files
            list_source = self.ctx.web.get_folder_by_server_relative_url(folder_url)
            folders = list_source.folders
            self.ctx.load(folders)
            self.ctx.execute_query()

            return folders

        except Exception as e:
            print(e)

    def get_filelist_sharepoint(self, file_url):
        '''
        return list of file_objects.
        exmaple get names:
        for file in files:
            print(file.properties["Name"])
        '''
        try:
            # file_url is the sharepoint url from which you need the list of files
            list_source = self.ctx.web.get_folder_by_server_relative_url(file_url)
            files = list_source.files
            self.ctx.load(files)
            self.ctx.execute_query()

            return files

        except Exception as e:
            print(e)
            
    def write_file_sharepoint(self, byte_data, file_url):
        '''
        write binary data into file on sharepoint
        '''
        #TODO: Check if file exists, Flag for overwrite existing file
        #TODO: Use sessions to support larger file upload/download
        
        if not(isinstance(byte_data, (bytes))):
            type(byte_data)
            raise RuntimeError('Data to write in files should be of type \'bytes\'')
            return

        file_name = os.path.basename(file_url)
        folder_url = os.path.dirname(file_url) + '/'
        libraryRoot = self.ctx.web.get_folder_by_server_relative_url(folder_url)

        target_file = libraryRoot.upload_file(file_name, byte_data).execute_query()
        print(f"File has been uploaded to url: {target_file.serverRelativeUrl}")
        
    def read_file_sharepoint(self, file_url):
        '''
        read file from sharpoint as binary data
        '''
        #TODO: Use sessions to support larger file upload/download
        try:
            file_response = File.open_binary(self.ctx, file_url)
            
            if file_response.status_code == 200:
                return file_response.content
            else:
                raise RuntimeError(f'error reading file {file_url} with status_code {string(file_response.status_code)} \n               Reason: {file_response.reason}')
        
        except Exception as e:
            print(e)
            
    def delete_file_sharepoint(self, file_url):
        '''
        delete file from sharpoint
        '''

        try:
            file_to_delete = self.ctx.web.get_file_by_server_relative_url(file_url)  
            file_to_delete.delete_object()
            self.ctx.execute_query()
            print(f"File has been delete: {file_url}")
        
        except Exception as e:
            print(e)       
    
    def write_file_local(self, byte_data, file_url):
        '''
        write binary data into local file
        '''
        if not(isinstance(byte_data, (bytes))):
            type(byte_data)
            raise RuntimeError('Data to write in files should be of type \'bytes\'')
            return
        
        try:
            #Saving file to local
            with open(file_url, 'wb') as output_file:  
                output_file.write(byte_data)
        except Exception as e:
            print(e)
            
    def read_file_local(self, file_url):
        '''
        read file from sharpoint as binary data
        '''
        try:
            #Read file from local
            with open(file_url, 'rb') as output_file:  
                file_data = output_file.read()
            return file_data
        except Exception as e:
            print(e)

###########  List related methods    ##################
            
    def connect_list(self, list_url):
        '''
        Connect to Sharepoint list
        list_url format: /Lists/ListName
        '''
        try:
            self.list = self.ctx.web.get_list(list_url)
            self.ctx.load(self.list)
            self.ctx.execute_query()
            print('Connected to List: ' + self.list.properties['Title'])
        except Exception as e:
            print(f'Could not connect to List: {list_url} - check if list exists')
            print(e)
    
    def is_list_connected(self):
        return self.list != None
        
    def get_list_connection(self):
        '''
        return the list object
        '''
        return self.list
    
    def read_list_sharepoint(self, list_url):
        '''
        Read complete list and returns content into Pandas dataframe
        '''
        # if not(self.is_list_connected()):
        self.connect_list(list_url)
            
        items = self.list.get_items().get_all().execute_query()
        return pd.DataFrame(items.to_json())
    
    def add_item_list_sharepoint(self,list_url,df):
        '''
        Add content from pandas dataframe into SharePoint list
        '''
        if not(self.is_list_connected()):
            self.connect_list(list_url)    
        
        for i, r in df.iloc[0:100,:].iterrows():
            content = r.to_dict()
            print(content)
            self.list.add_item(content)
        self.list.execute_query()
          