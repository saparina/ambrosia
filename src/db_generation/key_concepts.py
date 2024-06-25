import inflect

def get_plural(words):
    p_engine = inflect.engine()
    return p_engine.plural(words.lower()).capitalize()

class AttachmentConcepts:
    def __init__(self, general_class=None, class1=None, class2=None, 
                 common_property=None, 
                 common_value=None, 
                 template=None):
        """
        Template: [Class 1] and [Class 2] are subclasses of [General Class]. All [Entities of Class 1] and [Entities of Class 2] have property "[Common Property]". There might be a [Entity of Class 1] and a [Entity of Class 2] that both have "[Common Property]" equal to "[Common Value]".
        """
        self.class1 = class1
        self.class2 = class2
        self.general_class = general_class
        self.common_property = common_property
        self.common_value = common_value
        self.template = template

    def __hash__(self):
        return hash((self.class1, self.class2, self.common_property))

    def __eq__(self, other):
        if not isinstance(other, type(self)): return NotImplemented
        return self.class1 == other.class1 and \
            self.class2 == other.class2 and self.common_property == other.common_property and self.common_value == other.common_value
    
    def __repr__(self):  
        return "\t".join([f"general_class: {self.general_class}",
                f"class1: {self.class1}",
                f"class2: {self.class2}",
                f"common_property: {self.common_property}",
                f"common_value: {self.common_value}"])
    
    def __str__(self):  
        return "\t".join([f"general_class: {self.general_class}",
                f"class1: {self.class1}",
                f"class2: {self.class2}",
                f"common_property: {self.common_property}",
                f"common_value: {self.common_value}"])

    def load_json(self, kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)
        return self
    
    def dump_json(self):
        return {
                "template": self.template,
                "general_class": self.general_class,
                "class1": self.class1,
                "class2": self.class2,
                "common_property": self.common_property if self.common_property else "",
                "common_value": self.common_value if self.common_value else ""
            }

class ScopeConcepts:
    def __init__(self, entities=None, components=None, specific_component=None, template=None):
        """
        Template: Each [Entity] has many different [Components]. Among them, [Specific Component] is common to many [Entities].
        """
        self.specific_component = specific_component
        self.components = components
        self.entities = entities
        if template:
            self.template = template
        else:
            self.template = f"Each {self.entities} has many different {self.components}. Among them, {self.specific_component} is common to many {self.entities}."

    def __hash__(self):
        return hash((self.entities, self.components, self.specific_component))

    def __eq__(self, other):
        if not isinstance(other, type(self)): return NotImplemented
        return self.entities == other.entities and self.components == other.components and self.specific_component == other.specific_component
    
    def __repr__(self):  
        return "\t".join([f"entities: {self.entities}",
                f"components: {self.components}",
                f"specific_component: {self.specific_component}"])
    
    def __str__(self):  
        return "\t".join([f"entities: {self.entities}",
                f"components: {self.components}",
                f"specific_component: {self.specific_component}"])
    
    def load_json(self, kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)
        return self
    
    def dump_json(self):
        return {
                "template": self.template,
                "specific_component": self.specific_component,
                "components": self.components,
                "entities": self.entities
                }
    

class VagueConcepts:
    def __init__(self, subject=None, general_category1=None, general_category2=None, focus=None, template=None):
        """
        Template: Question: [Who / What / How / When / Where ...]? Subject of Inquiry: [Subject]
                  Focus: [Focus] Possible answer types: 1. [General Category 1] 2. [General Category 2]
        """
        self.template = template
        self.general_category1 = general_category1
        self.general_category2 = general_category2
        self.subject = subject
        self.focus = focus

    def __hash__(self):
        return hash((self.subject, self.general_category1, self.general_category2, self.focus))

    def __eq__(self, other):
        if not isinstance(other, type(self)): return NotImplemented
        return self.subject == other.subject and self.general_category1 == other.general_category1 and self.general_category2 == other.general_category2 and self.focus == other.focus
    
    def __repr__(self):  
        return "\t".join([f"subject: {self.subject}",
                        f"focus: {self.focus}",
                        f"general_category1: {self.general_category1}",
                        f"general_category2: {self.general_category2}"
                ])
    
    def __str__(self):  
        return "\t".join([f"subject: {self.subject}",
                            f"focus: {self.focus}",
                            f"general_category1: {self.general_category1}",
                            f"general_category2: {self.general_category2}"])

    def load_json(self, kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)
        return self
    
    def dump_json(self):
        return {
                "template": self.template,
                "subject": self.subject,
                "focus": self.focus,
                "general_category1": self.general_category1,
                "general_category2": self.general_category2
                }

class DBItem:
    def __init__(self, tab_name=None, col_name=None, value=None, scope_id=None):
        self.tab_name = tab_name
        self.col_name = col_name
        self.value = value
        self.scope_id = scope_id
        if value:
            self.db_type = "value"
        elif col_name:
            self.db_type = "column"
        else:
            self.db_type = "table"

    def load_json(self, kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)
        return self
    
    def get_name(self):
        if self.db_type == "value":
            return self.value
        elif self.db_type == "column":
            return self.col_name
        else:
            return self.tab_name

    def __repr__(self):  
        return "\t".join([f"name: {self.get_name()}",
                f"db_type: {self.db_type}"])
    
    def __str__(self):  
        return "\t".join([f"name: {self.get_name()}",
                f"db_type: {self.db_type}"])
    
    def __eq__(self, other):
        if not isinstance(other, type(self)): return NotImplemented
        return self.tab_name == other.tab_name and self.col_name == other.col_name and self.value == other.value
    
class AttachmentConceptsDB:
    def __init__(self, general_class=None, class1=None, class2=None, common_property=None, domain=None, template=None, type=None):
        self.class1 = class1
        self.class2 = class2
        self.general_class = general_class
        self.common_property = common_property
        self.domain = domain
        self.template = template
        self.type = type

        if self.general_class is not None and self.class1 is not None and self.class2 is not None:
            if isinstance(self.general_class, DBItem):
                self.general_class_name = self.general_class.get_name()
            else:
                self.general_class_name = self.general_class
            self.concepts = AttachmentConcepts(self.general_class_name, self.class1.get_name(), self.class2.get_name(), template=self.template)
            self.concepts.common_property = common_property
            

    def load_json(self, json_item):
        self.domain = json_item["domain"]
        if isinstance(json_item["general_class"], dict):
            self.general_class = DBItem().load_json(json_item["general_class"])
        else:
            self.general_class = json_item["general_class"]
        self.class1 = DBItem().load_json(json_item["class1"])
        self.class2 = DBItem().load_json(json_item["class2"])
        self.common_property = DBItem().load_json(json_item["common_property"])
        self.template = json_item["template"]
        self.type = json_item["type"]
        if isinstance(self.general_class, DBItem):
            self.general_class_name = self.general_class.get_name()
        else:
            self.general_class_name = self.general_class
        self.concepts = AttachmentConcepts(self.general_class_name, self.class1.get_name(), self.class2.get_name(), self.common_property.get_name(), template=self.template)
        self.concepts.common_property = self.common_property
        return self
    
    def dump_json(self):
        return {
                "domain": self.domain,
                "template": self.template,
                "type": self.type,
                "general_class": self.general_class.__dict__ if isinstance(self.general_class, DBItem) else self.general_class,
                "class1": self.class1.__dict__,
                "class2": self.class2.__dict__,
                "common_property": self.common_property.__dict__,
                }
    
    def __repr__(self):  
        return "\n".join([f"domain: {self.domain}",
                f"general_class: {self.general_class}",
                f"class1: {self.class1}",
                f"class2: {self.class2}",
                f"common_property: {self.common_property}"])
    
    def __str__(self):  
        return "\n".join([f"domain: {self.domain}",
                f"general_class: {self.general_class}",
                f"class1: {self.class1}",
                f"class2: {self.class2}"
                f"common_property: {self.common_property}"])



class VagueConceptsDB:
    def __init__(self, subject=None, general_category1=None, general_category2=None, template=None, type=None):
        self.general_category1 = general_category1
        self.general_category2 = general_category2
        self.subject = subject
        self.template = template
        self.type = type
        
        if self.general_category1 is not None and self.general_category2 is not None and self.subject is not None:
            self.concepts = VagueConcepts(self.general_category1.get_name(), self.general_category2.get_name(), self.subject.get_name(), template=self.template)


    def __repr__(self):  
        return "\t".join([f"subject: {self.subject}",
                f"general_category1: {self.general_category1}",
                f"general_category2: {self.general_category2}"])
    
    def __str__(self):  
        return "\t".join([f"subject: {self.subject}",
                f"general_category1: {self.general_category1}",
                f"general_category2: {self.general_category2}"])

    def load_json(self, json_item):
        self.template = json_item["template"]
        self.general_category1 = DBItem().load_json(json_item["general_category1"])
        self.general_category2 = DBItem().load_json(json_item["general_category2"])
        self.subject = DBItem().load_json(json_item["subject"])
        self.concepts = VagueConcepts(self.general_category1.get_name(), self.general_category2.get_name(), self.subject.get_name(), template=self.template)
        self.type = json_item["type"]
        return self
    
    def dump_json(self):
        return {
                "template": self.template,
                "general_category1": self.general_category1.__dict__,
                "general_category2": self.general_category2.__dict__,
                "subject": self.subject.__dict__,
                "type": self.type
                }
    
    def __repr__(self):  
        return "\t".join([f"subject: {self.subject}",
                f"general_category1: {self.general_category1}",
                f"general_category2: {self.general_category2}"])
    
    def __str__(self):  
        return "\t".join([f"subject: {self.subject}",
                f"general_category1: {self.general_category1}",
                f"general_category2: {self.general_category2}"])



class ScopeConceptsDB:
    def __init__(self, entities=None, components=None, specific_component=None, entities_components=None, template=None):
        self.entities = entities
        self.components = components
        self.specific_component = specific_component
        self.entities_components = entities_components
        self.template = template

        if self.entities is not None and self.components is not None and self.specific_component is not None:
            self.concepts = ScopeConcepts(self.entities.get_name(), self.components.get_name(), self.specific_component.get_name(), template=self.template)


    def __repr__(self):  
        return "\t".join([f"entities: {self.entities}",
                f"components: {self.components}",
                f"specific_component: {self.specific_component}",
                f"entities_components: {self.entities_components}"])
    
    def __str__(self):  
        return "\t".join([f"entities: {self.entities}",
                f"components: {self.components}",
                f"specific_component: {self.specific_component}",
                f"entities_components: {self.entities_components}"])

    def load_json(self, json_item):
        self.template = json_item["template"]
        self.entities = DBItem().load_json(json_item["entities"])
        self.components = DBItem().load_json(json_item["components"])
        self.specific_component = DBItem().load_json(json_item["specific_component"])
        self.entities_components = DBItem().load_json(json_item["entities_components"])
        self.concepts = ScopeConcepts(self.entities.get_name(), self.components.get_name(), self.specific_component.get_name(), template=self.template)
        return self
    
    def dump_json(self):
        return {
                "template": self.template,
                "entities": self.entities.__dict__,
                "components": self.components.__dict__,
                "specific_component": self.specific_component.__dict__,
                "entities_components": self.entities_components.__dict__
                }