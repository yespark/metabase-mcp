# metabase-mcp-mbql

Serveur MCP (Model Context Protocol) pour l'intégration avec Metabase, avec support complet de MBQL pour l'édition visuelle des requêtes.

## Installation

```bash
npm install metabase-mcp-mbql
```

## Configuration MCP

Ajoutez cette configuration à votre client MCP (Claude Desktop, etc.) :

```json
{
  "mcpServers": {
    "metabase": {
      "command": "npx",
      "args": ["metabase-mcp-mbql"],
      "env": {
        "METABASE_URL": "https://metabase.example.com",
        "METABASE_API_KEY": "mb_votre_cle_api"
      }
    }
  }
}
```

### Authentification

Deux méthodes sont supportées :

**1. Clé API (recommandé) :**
```json
{
  "METABASE_URL": "https://metabase.example.com",
  "METABASE_API_KEY": "mb_votre_cle_api"
}
```

**2. Identifiants utilisateur :**
```json
{
  "METABASE_URL": "https://metabase.example.com",
  "METABASE_USERNAME": "votre_email",
  "METABASE_PASSWORD": "votre_mot_de_passe"
}
```

---

## Outils disponibles

### Dashboards

| Outil | Description |
|-------|-------------|
| `list_dashboards` | Liste tous les dashboards |
| `get_dashboard` | Récupère les détails complets d'un dashboard (cartes, paramètres) |
| `create_dashboard` | Crée un nouveau dashboard |
| `update_dashboard` | Met à jour un dashboard existant |
| `delete_dashboard` | Archive ou supprime un dashboard |
| `get_dashboard_cards` | Liste toutes les cartes d'un dashboard |
| `add_card_to_dashboard` | Ajoute une carte à un dashboard |
| `remove_card_from_dashboard` | Retire une carte d'un dashboard |
| `update_dashboard_cards` | Met à jour les cartes avec leurs mappings de paramètres |
| `add_dashboard_filter` | Ajoute ou met à jour un filtre sur un dashboard |

### Cards (Questions)

| Outil | Description |
|-------|-------------|
| `list_cards` | Liste toutes les questions/cartes (filtres: `archived`, `table`, `database`, `using_model`, `bookmarked`, `using_segment`, `all`, `mine`) |
| `get_card` | Récupère une carte avec sa configuration complète (dataset_query, template-tags) |
| `create_card` | Crée une nouvelle question |
| `update_card` | Met à jour une question existante |
| `delete_card` | Archive ou supprime une question |
| `execute_card` | Exécute une question et retourne les résultats |

### Requêtes MBQL

| Outil | Description |
|-------|-------------|
| `create_card_mbql` | Crée une question en MBQL (éditable dans le query builder visuel) |
| `execute_mbql_query` | Exécute une requête MBQL sans créer de carte |

### Requêtes SQL

| Outil | Description |
|-------|-------------|
| `execute_query` | Exécute une requête SQL native sur une base de données |

### Bases de données

| Outil | Description |
|-------|-------------|
| `list_databases` | Liste toutes les bases de données |
| `get_database_metadata` | Récupère les métadonnées complètes (tables, champs, IDs) |
| `get_table_metadata` | Récupère les métadonnées détaillées d'une table |

### Collections

| Outil | Description |
|-------|-------------|
| `list_collections` | Liste toutes les collections |
| `create_collection` | Crée une nouvelle collection |
| `update_collection` | Met à jour une collection |

### Utilisateurs

| Outil | Description |
|-------|-------------|
| `list_users` | Liste tous les utilisateurs |
| `get_user` | Récupère les détails d'un utilisateur |
| `create_user` | Crée un nouvel utilisateur |
| `update_user` | Met à jour un utilisateur |
| `disable_user` | Désactive un utilisateur |

### Permissions

| Outil | Description |
|-------|-------------|
| `list_permission_groups` | Liste les groupes de permissions |
| `create_permission_group` | Crée un groupe de permissions |
| `delete_permission_group` | Supprime un groupe de permissions |
| `get_collection_permissions` | Récupère le graphe des permissions par collection |
| `update_collection_permissions` | Met à jour les permissions d'un groupe sur une collection |
| `add_user_to_group` | Ajoute un utilisateur à un groupe |
| `remove_user_from_group` | Retire un utilisateur d'un groupe |

---

## Exemples d'utilisation

### Créer une question MBQL

```json
{
  "name": "Ventes par mois",
  "database_id": 1,
  "query": {
    "source-table": 123,
    "aggregation": [["sum", ["field", 456, null]]],
    "breakout": [["field", 789, {"temporal-unit": "month"}]]
  },
  "display": "line"
}
```

### Configurer un filtre avec template-tag

```json
{
  "card_id": 42,
  "dataset_query": {
    "type": "native",
    "database": 1,
    "native": {
      "query": "SELECT * FROM orders WHERE status = {{status}}",
      "template-tags": {
        "status": {
          "id": "abc-123",
          "name": "status",
          "display-name": "Statut",
          "type": "dimension",
          "dimension": ["field", 456, null],
          "widget-type": "category"
        }
      }
    }
  }
}
```

### Connecter un filtre dashboard à une carte

```json
{
  "dashboard_id": 10,
  "cards": [{
    "id": 100,
    "card_id": 42,
    "row": 0,
    "col": 0,
    "size_x": 6,
    "size_y": 4,
    "parameter_mappings": [{
      "parameter_id": "filter-1",
      "card_id": 42,
      "target": ["variable", ["template-tag", "status"]]
    }]
  }]
}
```

---

## Développement

```bash
# Installation des dépendances
npm install

# Build
npm run build

# Watch mode
npm run watch

# Inspector MCP (debug)
npm run inspector
```

## Licence

MIT
