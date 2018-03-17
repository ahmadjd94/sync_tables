#!/usr/bin/python3
"""Author : Ahmad Da'na"""
from hashlib import sha256
from datetime import datetime
import time

from sqlalchemy import create_engine, MetaData, desc, Column, Integer, String,DateTime
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base

#Todo : improve loggin
class HashBase(object):
    pass

    @property
    def get_hash(self):
        hash_obj = sha256()
        hash_obj.update(str(self).encode())
        return hash_obj.hexdigest()

    def __init__(self):
        pass


if __name__ == "__main__":
    engine = create_engine('postgresql://root:pass@localhost/mesc')
    base = declarative_base()
    meta = MetaData()


    class Todo(base, HashBase):
        id = Column(Integer, primary_key=True)
        name = Column(String)
        description = Column(String)
        last_updated = Column(DateTime, default=datetime.now)
        __tablename__ = "todo"

        def __str__(self):
            return self.name+self.description


    class TodoMirror(base, HashBase):
        id = Column(Integer, primary_key=True)
        name = Column(String)
        description = Column(String)
        last_updated = Column(DateTime, default=datetime.now)
        __tablename__ = "todo_mirror"


    class ChangesTable(base):  # should contains every transaction (obj ID), type and date of occurence
        id = Column(Integer, autoincrement=True, primary_key=True)
        transaction_id = Column(Integer)
        transaction_type = Column(String)  # insertion\Deletion\Update
        date = Column(DateTime, default= datetime.now)
        current_hash = Column(String, unique=True)   # store hash of document
        __tablename__ = "changes"

        def __str__(self):
            return str(self.id) + self.transaction_type

    base.metadata.create_all(engine)

    Session = sessionmaker()
    Session.configure(bind=engine)
    session = Session()

    while True:
        table1_objs = session.query(Todo).all()
        table2_objs = session.query(TodoMirror).all()
        change_objs = session.query(ChangesTable).order_by(desc(ChangesTable.date)).all()

        set_a = set([table1_obj.id for table1_obj in table1_objs])
        set_b = set([table2_obj.id for table2_obj in table2_objs])
        set_c = set([change_obj.transaction_id for change_obj in change_objs])

        ids_in_a_not_b_or_c = set_a - set_b - set_c   # should detect freshly inserted entities
        ids_in_b_not_a_or_c = set_b - set_a - set_c

        ids_in_a_c_not_b = set_a.intersection(set_c) - set_b     # should detect deleted entites
        ids_in_b_c_not_a = set_b.intersection(set_c) - set_a

        ids_in_a_b_c = (set_a.intersection(set_b)).intersection(set_c)

        try:
            for table1_obj in table1_objs:
                obj_id = table1_obj.id
                obj_name = table1_obj.name
                obj_desc = table1_obj.description
                obj_hash = table1_obj.get_hash

                if obj_id in ids_in_a_not_b_or_c:
                    # no entities were found Insert it in Changes Table and the reflection table
                    target_entity = TodoMirror(id=obj_id, name=obj_name, description=obj_desc)
                    session.add(target_entity)
                    change_entity = ChangesTable(transaction_id=obj_id, transaction_type="INSERTION",
                                                 current_hash=table1_obj.get_hash)

                    session.add(change_entity)

                elif obj_id in ids_in_a_c_not_b:     # indicates deleted instance
                    change_obj = ChangesTable(transaction_id=obj_id, transaction_type="DELETE")

                    session.add(change_obj)
                    session.delete(table1_obj)

                elif obj_id in ids_in_a_b_c: # Indicates an entity was found in origin, reflection and changes tables
                    reflection_obj = session.query(TodoMirror).filter_by(id=obj_id).first()
                    if reflection_obj.get_hash == obj_hash:
                        print("No Update required")

                    elif reflection_obj.last_updated > table1_obj.last_updated:
                        session.query(Todo).filter_by(id=obj_id).update({
                                                    "name": reflection_obj.name,
                                                    "description": reflection_obj.description})

                    elif table1_obj.last_updated > reflection_obj.last_updated:
                        session.query(TodoMirror).filter_by(id=obj_id).update({
                                                          "name": reflection_obj.name,
                                                          "description": reflection_obj.description})
                        pass
                    else:
                        print("can't process two entities with different hashes modified at the same time")

            session.commit()

            for table2_obj in table2_objs:
                obj_id = table2_obj.id
                obj_name = table2_obj.name
                obj_desc = table2_obj.description
                obj_hash = table2_obj.get_hash

                if obj_id in ids_in_b_not_a_or_c:
                    # no entities were found Insert it in Changes Table and the reflection table
                    target_entity = Todo(id=obj_id, name=obj_name, description=obj_desc)
                    session.add(target_entity)
                    change_entity = ChangesTable(transaction_id=obj_id, transaction_type="INSERTION",
                                                 current_hash=table1_obj.get_hash)

                    session.add(change_entity)

                elif obj_id in ids_in_b_c_not_a:     # indicates deleted instance
                    change_obj = ChangesTable(transaction_id=obj_id, transaction_type="DELETE")

                    session.add(change_obj)
                    session.delete(table2_obj)

                elif obj_id in ids_in_a_b_c:    # Indicates an entity was found in origin, reflection and changes tables
                    reflection_obj = session.query(Todo).filter_by(id=obj_id).first()
                    if reflection_obj.get_hash == obj_hash:
                        print("No Update required")

                    elif reflection_obj.last_updated > table2_obj.last_updated:
                        session.query(Todo).filter_by(id=obj_id).update({"id": obj_id,
                                                                         "name": reflection_obj.name,
                                                                         "description": reflection_obj.description,
                                                                         "last_updated": datetime.now()})

                    elif table2_obj.last_updated > reflection_obj.last_updated:
                        session.query(TodoMirror).filter_by(id=obj_id).update(
                            {
                                "name": table2_obj.name,
                                "description": table2_obj.description,
                                "last_updated": datetime.now()
                            }
                        )

                        pass
                    else:
                        print("can't process two entites with different hashes modified at the same time")

            session.commit()

        except Exception as exc1:
            print(exc1, " value not found")
        print("sleeping")
        time.sleep(3)
