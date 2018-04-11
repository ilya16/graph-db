_tabversion = '3.8'

_lr_method = 'LALR'

_lr_signature = '5E0C4B0BBE2A68D7DACF60D089032C06'

_lr_action_items = {'RETURN': ([1, 4, 8, 10, 12, 24, 30, 34, 35, 36, 38, 40, 51, 56, 63, 66, 67, 68, 69, 72, ],
                               [6, 6, -28, -21, -26, -27, -23, -22, -24, -25, -1, -14, -2, -12, -3, -11, -10, -13, -9,
                                -4, ]), 'LBRACKET': ([17, 33, ], [31, 31, ]), 'LESSTHAN': (
[8, 10, 12, 30, 34, 35, 36, 38, 51, 63, 72, ], [18, -21, 18, 18, 18, 18, 18, -1, -2, -3, -4, ]), 'LEFT_ARROW': (
[8, 10, 12, 30, 34, 35, 36, 38, 51, 63, 72, ], [20, -21, 20, 20, 20, 20, 20, -1, -2, -3, -4, ]),
                    'DOT': ([13, 14, 28, 29, 41, 44, 45, 46, ], [26, 27, 26, 27, 26, 27, -15, -16, ]), 'RPAREN': (
    [23, 37, 52, 55, 56, 62, 66, 67, 68, 69, 77, 78, 79, 80, ],
    [38, 51, 63, 68, -12, 72, -11, -10, -13, -9, -6, -8, -5, -7, ]),
                    'RCURLEY': ([75, 77, 78, 79, 80, ], [80, -6, -8, -5, -7, ]), 'CREATE': ([0, ], [2, ]), 'COMMA': (
    [7, 8, 10, 11, 12, 13, 14, 28, 29, 30, 34, 35, 36, 38, 45, 46, 51, 62, 63, 72, 75, 77, 78, 79, 80, ],
    [15, 19, -21, 15, 19, -32, -33, -34, -35, 19, 19, 19, 19, -1, -15, -16, -2, 73, -3, -4, 73, 73, 73, -5, -7, ]),
                    'RIGHT_ARROW': (
                    [8, 10, 12, 30, 34, 35, 36, 38, 51, 63, 72, ], [16, -21, 16, 16, 16, 16, 16, -1, -2, -3, -4, ]),
                    'COLON': ([9, 23, 31, 48, 64, ], [22, 39, 47, 59, 74, ]), '$end': (
    [3, 4, 7, 8, 10, 11, 13, 14, 28, 29, 30, 34, 35, 36, 38, 45, 46, 51, 63, 72, ],
    [0, -30, -29, -28, -21, -31, -32, -33, -34, -35, -23, -22, -24, -25, -1, -15, -16, -2, -3, -4, ]),
                    'STRING': ([57, 74, ], [69, 79, ]), 'EQUALS': ([44, 45, 46, ], [57, -15, -16, ]), 'DASH': (
    [8, 10, 12, 18, 30, 32, 34, 35, 36, 38, 50, 51, 63, 70, 72, 76, ],
    [17, -21, 17, 33, 17, 49, 17, 17, 17, -1, 61, -2, -3, -17, -4, -18, ]), 'GREATERTHAN': ([49, ], [60, ]), 'LPAREN': (
    [2, 5, 16, 19, 20, 21, 25, 42, 43, 53, 54, 60, 61, ], [9, 9, 9, 9, 9, 9, 42, 42, 42, 42, 42, -19, -20, ]),
                    'WHERE': (
                    [10, 12, 30, 34, 35, 36, 38, 51, 63, 72, ], [-21, 25, -23, -22, -24, -25, -1, -2, -3, -4, ]),
                    'MATCH': ([0, ], [5, ]), 'AND': ([40, 55, 56, 66, 67, 68, 69, ], [53, 53, 53, 53, 53, -13, -9, ]),
                    'NAME': ([22, 39, 47, 59, ], [37, 52, 58, 71, ]), 'KEY': (
    [6, 9, 15, 25, 26, 27, 31, 42, 43, 52, 53, 54, 65, 73, 74, ],
    [13, 23, 28, 41, 45, 46, 48, 41, 41, 64, 41, 41, 64, 64, 64, ]),
                    'NOT': ([25, 42, 43, 53, 54, ], [43, 43, 43, 43, 43, ]), 'RBRACKET': ([58, 71, ], [70, 76, ]),
                    'LCURLEY': ([52, 65, 73, 74, ], [65, 65, 65, 65, ]),
                    'OR': ([40, 55, 56, 66, 67, 68, 69, ], [54, 54, 54, 54, 54, -13, -9, ]), }

_lr_action = {}
for _k, _v in _lr_action_items.items():
    for _x, _y in zip(_v[0], _v[1]):
        if not _x in _lr_action:  _lr_action[_x] = {}
        _lr_action[_x][_k] = _y
del _lr_action_items

_lr_goto_items = {'match_where': ([0, ], [1, ]), 'constraint': ([25, 42, 43, 53, 54, ], [40, 55, 56, 66, 67, ]),
                  'literals': ([2, 5, 16, 19, 20, 21, ], [8, 12, 30, 34, 35, 36, ]), 'where_clause': ([12, ], [24, ]),
                  'edge_condition': ([17, 33, ], [32, 50, ]), 'full_query': ([0, ], [3, ]),
                  'return_variables': ([1, 4, ], [7, 11, ]), 'condition_list': ([52, 65, 73, 74, ], [62, 75, 77, 78, ]),
                  'node_clause': ([2, 5, 16, 19, 20, 21, ], [10, 10, 10, 10, 10, 10, ]),
                  'create_clause': ([0, ], [4, ]),
                  'labeled_edge': ([8, 12, 30, 34, 35, 36, ], [21, 21, 21, 21, 21, 21, ]),
                  'keypath': ([6, 15, 25, 42, 43, 53, 54, ], [14, 29, 44, 44, 44, 44, 44, ]), }

_lr_goto = {}
for _k, _v in _lr_goto_items.items():
    for _x, _y in zip(_v[0], _v[1]):
        if not _x in _lr_goto: _lr_goto[_x] = {}
        _lr_goto[_x][_k] = _y
del _lr_goto_items
_lr_productions = [
    ("S' -> full_query", "S'", 1, None, None, None),
    ('node_clause -> LPAREN KEY RPAREN', 'node_clause', 3, 'p_node_clause', 'cypher_parser.py', 181),
    ('node_clause -> LPAREN COLON NAME RPAREN', 'node_clause', 4, 'p_node_clause', 'cypher_parser.py', 182),
    ('node_clause -> LPAREN KEY COLON NAME RPAREN', 'node_clause', 5, 'p_node_clause', 'cypher_parser.py', 183),
    (
    'node_clause -> LPAREN KEY COLON NAME condition_list RPAREN', 'node_clause', 6, 'p_node_clause', 'cypher_parser.py',
    184),
    ('condition_list -> KEY COLON STRING', 'condition_list', 3, 'p_condition', 'cypher_parser.py', 202),
    ('condition_list -> condition_list COMMA condition_list', 'condition_list', 3, 'p_condition', 'cypher_parser.py',
     203),
    ('condition_list -> LCURLEY condition_list RCURLEY', 'condition_list', 3, 'p_condition', 'cypher_parser.py', 204),
    ('condition_list -> KEY COLON condition_list', 'condition_list', 3, 'p_condition', 'cypher_parser.py', 205),
    ('constraint -> keypath EQUALS STRING', 'constraint', 3, 'p_constraint', 'cypher_parser.py', 218),
    ('constraint -> constraint OR constraint', 'constraint', 3, 'p_constraint', 'cypher_parser.py', 219),
    ('constraint -> constraint AND constraint', 'constraint', 3, 'p_constraint', 'cypher_parser.py', 220),
    ('constraint -> NOT constraint', 'constraint', 2, 'p_constraint', 'cypher_parser.py', 221),
    ('constraint -> LPAREN constraint RPAREN', 'constraint', 3, 'p_constraint', 'cypher_parser.py', 222),
    ('where_clause -> WHERE constraint', 'where_clause', 2, 'p_where_clause', 'cypher_parser.py', 240),
    ('keypath -> KEY DOT KEY', 'keypath', 3, 'p_keypath', 'cypher_parser.py', 248),
    ('keypath -> keypath DOT KEY', 'keypath', 3, 'p_keypath', 'cypher_parser.py', 249),
    (
    'edge_condition -> LBRACKET COLON NAME RBRACKET', 'edge_condition', 4, 'p_edge_condition', 'cypher_parser.py', 261),
    ('edge_condition -> LBRACKET KEY COLON NAME RBRACKET', 'edge_condition', 5, 'p_edge_condition', 'cypher_parser.py',
     262),
    ('labeled_edge -> DASH edge_condition DASH GREATERTHAN', 'labeled_edge', 4, 'p_labeled_edge', 'cypher_parser.py',
     273),
    ('labeled_edge -> LESSTHAN DASH edge_condition DASH', 'labeled_edge', 4, 'p_labeled_edge', 'cypher_parser.py', 274),
    ('literals -> node_clause', 'literals', 1, 'p_literals', 'cypher_parser.py', 286),
    ('literals -> literals COMMA literals', 'literals', 3, 'p_literals', 'cypher_parser.py', 287),
    ('literals -> literals RIGHT_ARROW literals', 'literals', 3, 'p_literals', 'cypher_parser.py', 288),
    ('literals -> literals LEFT_ARROW literals', 'literals', 3, 'p_literals', 'cypher_parser.py', 289),
    ('literals -> literals labeled_edge literals', 'literals', 3, 'p_literals', 'cypher_parser.py', 290),
    ('match_where -> MATCH literals', 'match_where', 2, 'p_match_where', 'cypher_parser.py', 329),
    ('match_where -> MATCH literals where_clause', 'match_where', 3, 'p_match_where', 'cypher_parser.py', 330),
    ('create_clause -> CREATE literals', 'create_clause', 2, 'p_create', 'cypher_parser.py', 340),
    ('full_query -> match_where return_variables', 'full_query', 2, 'p_full_query', 'cypher_parser.py', 345),
    ('full_query -> create_clause', 'full_query', 1, 'p_full_query', 'cypher_parser.py', 346),
    ('full_query -> create_clause return_variables', 'full_query', 2, 'p_full_query', 'cypher_parser.py', 347),
    ('return_variables -> RETURN KEY', 'return_variables', 2, 'p_return_variables', 'cypher_parser.py', 354),
    ('return_variables -> RETURN keypath', 'return_variables', 2, 'p_return_variables', 'cypher_parser.py', 355),
    ('return_variables -> return_variables COMMA KEY', 'return_variables', 3, 'p_return_variables', 'cypher_parser.py',
     356),
    ('return_variables -> return_variables COMMA keypath', 'return_variables', 3, 'p_return_variables',
     'cypher_parser.py', 357),
]