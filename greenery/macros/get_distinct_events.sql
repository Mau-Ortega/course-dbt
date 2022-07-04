{%- macro get_distinct_events() -%}
    {% for event_type in  event_types['event_type'] %}
    sum(case when event_type = {{ event_type }} then 1 else 0 end) as {{ event_types['column_name'][loop.index0] }} 
    {% endfor %} 
 
{%- endmacro -%}