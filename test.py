with open("fields.csv", "r") as csvfile:
    reader = csv.DictReader(csvfile)
    namespaces = {}

    for row in reader:
        namespace = row["namespace"]
        property = row["property"]
        field = row["field"]
        type = row["type"]

        if namespace not in namespaces:
            namespaces[namespace] = []

        namespaces[namespace].append(f"{property} = Field('{field}', {type})")

# Generate the schema.py file
with open("schema.py", "w") as schema_file:
    schema_file.write("from pydantic import BaseModel, Field\n\n")

    for namespace, properties in namespaces.items():
        schema_file.write(f"class {namespace}(BaseModel):\n")
        for prop in properties:
            schema_file.write(f"    {prop}\n")
        schema_file.write("\n")