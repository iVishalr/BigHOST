import os
from typing import Any
from dotenv import load_dotenv
from pymongo import MongoClient

class DataBaseHandler:
    """
    Handler for interacting with MongoDB
    """
    def __init__(self) -> None:
        
        load_dotenv()
        client_rr = MongoClient(os.getenv('MONGO_URI_RR'), connect=False)
        client_ec = MongoClient(os.getenv('MONGO_URI_EC'), connect=False)
        
        db_rr = client_rr['bd']
        dbuser_rr = db_rr['user']
        assignment_rr = db_rr['assignmentQuestion']
        submissions_rr = db_rr['submissions']

        db_ec = client_ec['bd']
        dbuser_ec = db_ec['user']
        assignment_ec = db_ec['assignmentQuestion']
        submissions_ec = db_ec['submissions']

        self.metadata = {
            "RR": {
                "client": client_rr,
                "db": db_rr,
                "collections": {
                    "users": dbuser_rr,
                    "submissions": submissions_rr,
                    "assignmentQuestion": assignment_rr
                }
            },
            "EC": {
                "client": client_ec,
                "db": db_ec,
                "collections": {
                    "users": dbuser_ec,
                    "submissions": submissions_ec,
                    "assignmentQuestion": assignment_ec
                }
            }
        }

    def insert_one(self, db: str, collection: str, data: Any) -> int:
        if db not in self.metadata.keys():
            return -1
        
        if collection not in self.metadata[db]["collections"].keys():
            return -2
        
        self.metadata[db]["collections"][collection].insert_one(data)
        return 0

    def find_one(self, db: str, collection: str, find_filter: Any) -> Any:
        if db not in self.metadata.keys():
            return -1
        
        if collection not in self.metadata[db]["collections"].keys():
            return -2
        
        return self.metadata[db]["collections"][collection].find_one(find_filter)

    def try_find_one(self, db: str, collection: str, find_filter: Any) -> Any:
        if db not in self.metadata.keys():
            return -1
        
        if collection not in self.metadata[db]["collections"].keys():
            return -2

        return self.metadata[db]["collections"][collection].count_documents(find_filter, limit=1)

    def find_one_and_update(self, db: str, collection: str, find_filter: Any, update: Any) -> Any:
        if db not in self.metadata.keys():
            return -1
        
        if collection not in self.metadata[db]["collections"].keys():
            return -2

        document = self.metadata[db]["collections"][collection].find_one_and_update(find_filter, update)
        return document

    def close(self) -> None:
        self.db_metadata["RR"]["client"].close()
        self.db_metadata["EC"]["client"].close()

class DataBase(DataBaseHandler):
    def __init__(self) -> None:
        super().__init__()

    def gen_blacklisted_record(self, status: bool, message: str, timestamp: int) -> Any:
        record = {
            'status': status,
            'message': message,
            'timestamp': timestamp
        }
        return record

    def try_find(self, collection: str, teamId: str) -> Any:
        db = "RR"
        if '2' == teamId[2]:
            db = "EC"
        
        find_filter = {'teamId': teamId}
        return self.try_find_one(db, collection, find_filter)
    
    def insert(self, collection: str, teamId: str, blacklisted: dict, assignmentId: str, submissionId: str, marks: int, message: str, timestamp: int) -> Any:
        if blacklisted is None:
            blacklisted = self.gen_blacklisted_record(False, "", timestamp)
        
        doc = {
            'teamId': teamId,
            'blacklisted': blacklisted,
            'assignments': {
                assignmentId: {
                    submissionId: {
                        'marks': marks,
                        'message': message,
                        'timestamp': timestamp
                    }
                }
            }
        }
        
        db = "RR"
        if '2' == teamId[2]:
            db = "EC"

        return self.insert_one(db, collection, doc)
    
    def update(self, collection: str, teamId: str, blacklisted: dict, assignmentId: str, submissionId: str, marks: int, message: str, timestamp: int) -> Any:
        db = "RR"
        if '2' == teamId[2]:
            db = "EC"
        find_filter = {'teamId': teamId}
        doc = self.find_one(db, collection, find_filter)

        record = {
            'marks': marks,
            'message': message,
            'timestamp': timestamp
        }

        doc['assignments'][assignmentId]['submissions'][submissionId] = record

        update_filter = {
            "valid": {
                '$set': {
                    'assignments': doc['assignments'],
                }
            },
            "blacklisted": {
                '$set': {
                    'assignments': doc['assignments'],
                    'blacklisted': blacklisted
                }
            }
        }

        update = update_filter['blacklisted'] if blacklisted else update_filter['valid']
        return self.find_one_and_update(db, collection, find_filter, update)

    def unblacklist(self, collection: str, teamId: str, message: str, timestamp: int):
        db = "RR"
        if '2' == teamId[2]:
            db = "EC"
        blacklisted = self.gen_blacklisted_record(False, message, timestamp)
        find_filter = {'teamId': teamId}
        update_filter = {
            '$set': {
                'blacklisted': blacklisted
            }
        }
        return self.find_one_and_update(db, collection, find_filter, update_filter)

    def close_assignment(self, assignmentId: str, message: str):
        record = {
            'assignmentOpen': False,
            'assignmentClosedMessage': message
        }
        find_filter = {'assignmentId': assignmentId}
        update_filter = {'$set': {'assignmentOpen': record['assignmentOpen'], 'assignmentClosedMessage': record['assignmentClosedMessage']}}
        
        rr = self.find_one_and_update("RR", "assignmentQuestion", find_filter, update_filter)
        ec = self.find_one_and_update("EC", "assignmentQuestion", find_filter, update_filter)

        if rr < 0 or ec < 0:
            return -1
        else:
            return 0
