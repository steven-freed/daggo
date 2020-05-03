import smtplib
from email.message import EmailMessage
from typing import Type, Callable, Dict, List, Union, Tuple, Set, TypeVar

from nodes.dagnode import DAGNode


class EmailNode(DAGNode):
    '''
    Base class for DAGNodes basic requirements
    '''
    def __init__(self, ident: str, mailto: str, mailfrom: str,
                subject: str, body: str, provider: str = None) -> None:
        super(EmailNode, self).__init__(ident, self.sendMail)
        self.mailto = mailto
        self.mailfrom = mailfrom
        self.subject = subject
        self.body = body
        self.provider = provider
        self.providers = {
            None:    {'host': 'localhost', 'port': 3000, 'setup': lambda:None},
            'gmail': {'host': 'smtp.gmail.com', 'port': 587, 'setup': lambda:None},
        }
    
    def sendMail(self):
        self.providers[self.provider]['setup']()
        host = self.providers[self.provider]['host']
        port = self.providers[self.provider]['port']
        server = smtplib.SMTP(host, port)
        msg = EmailMessage()
        msg.set_content(self.body)
        msg['Subject'] = self.subject
        msg['From'] = self.mailfrom
        msg['To'] = self.mailto
        server.send_message(msg)
