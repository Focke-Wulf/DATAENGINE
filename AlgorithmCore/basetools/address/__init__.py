# Function Address Package import manage 
from .finalseg.prob_start import P as start_P
from .finalseg.prob_trans import P as trans_P
from .finalseg.prob_emit import P as emit_P
from . import segmentation as nlp
import json
__all__=["start_P","trans_P","emit_P","nlp","json"]