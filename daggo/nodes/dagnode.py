from typing import Type, Callable, Dict, List, Union, Tuple, Set, TypeVar


class DAGNode:
    '''
    Base class for DAGNodes basic requirements
    '''
    def __init__(self, ident: str, callee: Callable) -> None:
        self.ident = ident
        self.callee = callee
        self.downstream = []