# secrets.yaml
Access will not work without a secrets.yaml file in /pipeline/projects/your-project-name/config/secrets.yaml

secrets.yaml is registered with the .gitignore for security.

// Example secrets.yaml:
```
eds_apis:
  MyServer1:
    url: "http://127.0.0.1:43084/api/v1/"
    username: "admin"
    password: ""
  MyServer2:
    url: "http://some-ip-address:port/api/v1/"
    username: "admin"
    password: ""

contractor_apis:
  MySpecialContractor:
    url: "https://contractor-api.com/v1/special/"
    client_id: "special-user"
    password: "2685steam"
```
```