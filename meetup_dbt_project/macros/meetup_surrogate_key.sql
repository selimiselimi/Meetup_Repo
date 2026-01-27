{% macro meetup_surrogate_key(field_list) %}
    md5(
      {% for f in field_list %}
        coalesce(cast({{ f }} as varchar), '')
        {% if not loop.last %} || '|' || {% endif %}
      {% endfor %}
    )
{% endmacro %}