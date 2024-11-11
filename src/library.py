import requests
import time
from datetime import datetime
import numpy as np
import json
from typing import Dict, List, Any, Iterable, Optional, Union
import os
from queue import PriorityQueue, Queue

class Base:
    base="https://api.semanticscholar.org/graph/v1"
    waitsec = 0.1
    _last_data = None

    @classmethod
    def _request(cls, url, params=None,json=None, retries=np.inf, wait=None, fn=requests.get):
        if wait is None:
            wait = cls.waitsec

        
        while True:
            retries -= 1
            if retries < 0 :
                print(f"Coudn'r receive request {url} {params}")
                return None
                
            data = fn(cls.base+url, params=params, json=json).json()
            cls._last_data = data

            if "code" in data and data.get("code") == '429':
                print(url, data)
                time.sleep(wait)
                continue
            
            if "message" in data:
                print(url, data)
                time.sleep(wait)
                continue

            if "error" in data:
                print(data)
                raise Exception("Error in response")
            else:
                return data   


class Author(Base):
    fields = ["name,paperCount,hIndex,citationCount"]
    
    def __init__(self, authorId, name=None, hIndex=None,paperCount=None,citationCount=None,
                  **kwargs):
        if authorId is None:
            raise Exception("Author Id shoud not be None")
        
        self.Id = authorId
        self.name = name
        self.h = hIndex
        self.paperCount = paperCount
        self.citationCount = citationCount

        pass

    def get_dict(self):
        return {
            "authorId": self.Id,
            "name" : self.name,
            "hIndex" : self.h,
            "paperCount": self.paperCount,
            "citationCount": self.citationCount
        }
    def zero(self):
        self.h = 0
        self.paperCount =0 
        self.citationCount = 0

    def incomplete(self):
        return any([self.name is None,self.h is None,self.paperCount is None, self.citationCount is None])

    def __repr__(self):
        return f"< Author {self.Id[:4]} {self.name} h:{self.h} cite:{self.citationCount} papers:{self.paperCount} >" 

class Authors(Base):
    authors : Dict[str, Author] = {}

    def __new__(cls, authorId) -> Union[Optional[Author], List[Optional[Author]]]:
        if isinstance(authorId, Iterable) and not isinstance(authorId, str):
            return [Authors.authors.get(aId) for aId in authorId]
        return Authors.authors.get(authorId)
    
    @classmethod
    def reset(cls):
        cls.authors = {}


    @classmethod
    def add(cls, author : Author) -> Author:

        if author.Id not in cls.authors:
            cls.authors[author.Id] = author
        return cls.authors[author.Id]
    
    @classmethod
    def stat(cls):
        x = len([a for a in cls.authors.values() if a.incomplete()])
        return f"{x}/{cls.size()}"
    
    @classmethod
    def all(cls):
        return cls.authors.values()

    @classmethod
    def size(cls):
        return len(cls.authors)
    
    @classmethod
    def fill_data(cls, force=True):
        incomplete_ids = [a.Id for a in cls.authors.values() if a.incomplete()]
        batch_size = 900

        if len(incomplete_ids) < batch_size and force is False:
            return
        if len(incomplete_ids) == 0:
            return
        
        print("Filling author data...", end="")

        for i in range(0, len(incomplete_ids), batch_size):
            batch = incomplete_ids[i:i + batch_size]

            data = cls._request('/author/batch',
                    params={'fields':  ",".join(Author.fields)},
                    json={"ids": batch},
                    fn=requests.post
                )
            for aId, line in zip(batch, data):
                if line is None:
                    cls.authors[aId].zero()
                    continue

                author = Author(**line)
                cls.authors[author.Id] = author
            print(".", end="")
        print()
            

class Paper(Base):
    fields = ["publicationDate","citationCount","referenceCount","title","url","year","authors"]
    
    def __init__(self,paperId, depth=0,title=None, publicationDate=None, citationCount=None, referenceCount=None, url=None,
                  year=None,
                  authors=None,
                  **kwargs):
        if paperId is None:
            raise Exception("paper Id shoud not be None")
        self.Id = paperId
        self.title = title
        self.citationCount = citationCount
        self.referenceCount = referenceCount
        self.url=url
        self._date = publicationDate

        self._citations = kwargs.get("citations")
        self._references = kwargs.get("references")
        
        self.year=year
        if authors and len(authors) > 0 and type(authors[0]) is dict:
            self.authors = [Authors.add(Author(**a)).Id for a in authors if a["authorId"] is not None]
        else:
            self.authors=authors

        self.depth = depth

        

    def merge(self, other):
        pass

    @classmethod
    def from_title(cls, title):
        url = "https://api.semanticscholar.org/graph/v1/paper/search/match"
        params = {
            "query": title,
            "fields": ",".join(cls.fields)
        }
        data = cls._request("/paper/search/match", params=params)['data'][0]

        return Library.add(Paper(**data))
    
    @property
    def citations(self):
        if self._citations:
            return self._citations 
        self._citations = self.get_papers(self.Id, "citations")
        return self._citations 
    
    @property
    def references(self):

        if self._references:
            return self._references 
        
        self._references = self.get_papers(self.Id, "references")
        
        return self._references
    
    @classmethod
    def get_papers(cls, paperId, endpoint):
        if endpoint=='citations':
            paper_type = 'citingPaper'
        elif endpoint=='references':
            paper_type = 'citedPaper'
        else:
            raise Exception("unknown endpoint")
        

        paper_ids = []
        offset = 0
        limit = 1000
        while offset is not None:
            if offset + limit >= 10000:
                break
            time.sleep(cls.waitsec)
            
            data = cls._request(
                f"/paper/{paperId}/{endpoint}?offset={offset}&limit={limit}"+
                "&fields="+",".join(cls.fields))
                
            
            for line in data['data']:
                if line[paper_type]["paperId"] is None:
                    continue

                paper = Paper(**line[paper_type])
                if paper.Id is None:
                    continue
                Library.add(paper)
                paper_ids.append(paper.Id)

            offset = data.get("next",None)
    #         print(data)

        return paper_ids
    
    def __repr__(self):
        return f"< {self.Id[:4]}...: {self.title} >"
    
    @property
    def age(self):
        if self.date is None:
            return 1000*9
        days = (datetime.today() - self.datetime).days
        return days
    
    @property
    def datetime(self):
        if self.date is None:
            return None
        return datetime.strptime(self.date, '%Y-%m-%d')

    @property
    def date(self):
        if self._date is not None:
            return self._date
        
        if self.year is not None:
            return f"{self.year}-06-01"
        
        return None
        
    
    @property
    def citation_rate(self):
        return self.citationCount / self.age
    
    @property
    def author_max_hindex(self):
        h = 0
        for author in Authors(self.authors):
            h = max(h, author.h)
        return h
    
    @property
    def author_mean_hindex(self):
        return np.mean([author.h for author in Authors(self.authors)])

    def _get_author_citationCount(self):
        return [author.citationCount for author in Authors(self.authors)]
    
    def _get_author_paperCount(self):
        return [author.paperCount for author in Authors(self.authors)]

    @property
    def first_author(self):
        if len(self.authors) == 0:
            return "???"
        return Authors(self.authors[0]).name
    
    @property
    def last_author(self):
        if len(self.authors) == 0:
            return "???"
        return Authors(self.authors[-1]).name
    

    @property
    def author_max_cite(self):
        return max([0]+self._get_author_citationCount())
    

    @property
    def author_mean_cite(self):
        if len(self.authors) == 0:
            return 0
        return np.mean(self._get_author_citationCount())
    
    def metric(self):
        return np.array([self.citation_rate, self.author_mean_cite, self.author_max_hindex])

    def get_dict(self):
        return {
            "paperId":self.Id, 
            "depth": self.depth,
            "title": self.title, 
            "publicationDate": self._date,
            "year": self.year,
            "authors": self.authors,
            "citationCount": self.citationCount,
            "referenceCount": self.referenceCount,
            "url": self.url,
            "references": self._references,
            "citations": self._citations
        }
    
class Library(Base):
    name = None
    papers: Dict[str, Paper] = {}
    authors = Authors

    @classmethod
    def _intersect(cls, paper_ids):
        return list(set(paper_ids).intersection(cls.papers))

    @classmethod
    def repair(cls):
        for paper in cls.papers.values():
            if paper._citations is not None:
                paper._citations = cls._intersect(paper._citations)
            if paper._references is not None:
                paper._references = cls._intersect(paper._references)
                

    @classmethod
    def get(cls, paperId) -> Union[Optional[Paper], List[Optional[Paper]]]:
        if isinstance(paperId, Iterable) and not isinstance(paperId, str):
            return [cls.papers[pId] for pId in paperId]
        return cls.papers[paperId]
    
    @classmethod
    def reset(cls):
        name = None
        cls.papers = {}
        cls.authors.reset()
    
    @classmethod
    def rename(cls, name):
        cls.name = name

    @classmethod
    def store(cls, overwrite=False):
        data = {"name": cls.name}
        data["papers"] = [paper.get_dict() for paper in cls.papers.values()]
        data["authors"] = [author.get_dict() for author in cls.authors.all()]
        path = f"{cls.name}.json"
        if os.path.exists(path) and overwrite is False:
            print("File exists, use overwrite=True")
            return
        with open(path, "w") as f:
            json.dump(data, f)

    @classmethod
    def load(cls, path):
        with open(path, "r") as f:
            data = json.load(f)
        
        cls.reset()
        
        cls.name = data["name"]

        for kwargs in data["papers"]:
            cls.add(Paper(**kwargs))
        
        for kwargs in data["authors"]:
            cls.authors.add(Author(**kwargs))


    @classmethod
    def add(cls, paper) -> Paper:
        if paper.Id in cls.papers:
            cls.papers[paper.Id].merge(paper)
        else:    
            cls.papers[paper.Id] = paper

        return cls.papers[paper.Id]

    @classmethod
    def __call__(cls, paperId):
        return cls.papers.get(paperId)
    
    @classmethod
    def info(cls):
        return {
            "name": cls.name,
            "papers": cls.size(),
            "authors": cls.authors.size()
        }
    
    @classmethod
    def size(cls):
        return len(cls.papers)
    
