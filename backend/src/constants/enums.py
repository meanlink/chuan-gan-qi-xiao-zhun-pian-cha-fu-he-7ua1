from enum import Enum
class Role(str, Enum):
    admin = 'admin'
    analyst = 'analyst'
    viewer = 'viewer'
class JobStatus(str, Enum):
    queued = 'queued'
    running = 'running'
    completed = 'completed'
    failed = 'failed'
class HandoverStatus(str, Enum):
    validating = 'validating'
    importing = 'importing'
    recovering = 'recovering'
    failed = 'failed'
    completed = 'completed'
