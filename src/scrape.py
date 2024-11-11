from typing import Dict, List, Any

from queue import PriorityQueue
from .library import Library, Base, Paper, Authors

class Node:
    def __init__(self, paperId, depth=0):
        self.paperId = paperId
        self.depth = depth
        self.h = self.paper.author_mean_hindex
        pass
    
    @property
    def paper(self):
        return Library.get(self.paperId)

    def key(self):
        return (self.depth, -self.h)
    def get_dict(self):
        return {"paperId": self.paperId, "depth": self.depth}
    
class Network(Base):
    def __init__(self):
        self.nodes : Dict[str,Node] = {}
        self.anchors = []
        
        pass

    def add_anchor(self, paperId, depth=0):
        self.anchors.append(paperId)
        self.nodes[paperId] = Node(paperId, depth)
    
 
        
    def walk(self, depth, citations=False, references=False, filter=None):
        self.pque = PriorityQueue()
        for node in self.nodes.values():
            self.pque.put((node.key(), node.paperId))
        
        while not self.pque.empty():
            
            _, top_pId = self.pque.get()
            
            node = self.nodes.get(top_pId)
            
            
            if node.depth > depth:
                continue
            print(f"nodes: {len(self.nodes)}\t queue: {self.pque.qsize()}\t depth: {node.depth}\t h: {node.h:0.1f}\t Lib: {Library.size()}")


            add_to_que = []
            new_paper_ids = []
            if citations:
                new_paper_ids += node.paper.citations
            if references:
                new_paper_ids += node.paper.references

            add_to_que = [pId for pId in new_paper_ids if pId not in self.nodes]
            
            Authors.fill_data(force=True)

            for new_pId in add_to_que:
                new_node = Node(new_pId, node.depth + 1)
                paper = new_node.paper
                if filter is not None and filter(paper) is False:
                    continue

                self.nodes[new_pId] = new_node
                if node.depth > depth:
                    continue
                self.pque.put((new_node.key(), new_pId))
                