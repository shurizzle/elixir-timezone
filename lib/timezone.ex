defrecord Timezone, name: nil, zones: [] do
  def last_weekday_of_the_month(year, month, weekday) do
    Enum.find :calendar.last_day_of_the_month(year, month)..1, fn(day) ->
      :calendar.day_of_the_week(year, month, day) == weekday
    end
  end

  def first_weekday_of_the_month(year, month, weekday, fun) do
    last = :calendar.last_day_of_the_month(year, month)
    first = Enum.find 1.. :calendar.last_day_of_the_month(year, month), fn(d) ->
      :calendar.day_of_the_week(year, month, d) == weekday
    end

    Enum.find (Enum.map 0..trunc((last - first) / 7), fn(x) ->
      x * 7 + first
    end), fun
  end

  def first_weekday_of_the_month_ge(year, month, weekday, day) do
    first_weekday_of_the_month year, month, weekday, &1 >= day
  end

  def first_weekday_of_the_month_le(year, month, weekday, day) do
    first_weekday_of_the_month year, month, weekday, &1 <= day
  end

  defrecord Zone, offset: 0, rule: nil, format: nil, until: nil do
  end

  defrecord Rule, year: nil, month: nil, day: nil, time: nil, save: nil, letters: nil do
    def year_contains(_, Rule[year: Range[first: :min, last: :max]]), do: true
    def year_contains(year, Rule[year: Range[first: :min, last: x]]) when x >= year, do: true
    def year_contains(year, Rule[year: Range[first: x, last: :max]]) when x <= year, do: true
    def year_contains(year, Rule[year: Range[first: x1, last: x2]]) when year >= x1 and year <= x2, do: true
    def year_contains(_, _), do: false

    def compile_for_year(year, Rule[] = self) do
      case year_contains(year, self) do
        true ->
          day =
            case self.day do
              {:last, d} -> Timezone.last_weekday_of_the_month(year, self.month, d)
              {:>=, d, i} -> Timezone.first_weekday_of_the_month_ge(year, self.month, d, i)
              {:<=, d, i} -> Timezone.first_weekday_of_the_month_le(year, self.month, d, i)
              d -> d
            end
          {{year, self.month, day}, self.time}
        false -> nil
      end
    end

    def contains({{y,_,_},{_,_,_}} = date, Rule[] = self) do
      case compile_for_year(y, self) do
        nil -> false
        d -> :calendar.datetime_to_gregorian_seconds(date) >= :calendar.datetime_to_gregorian_seconds(d)
      end
    end
  end

  defrecord RuleSet, name: nil, rules: [] do
    def in_range({{y,_,_},{_,_,_}}=first, last, RuleSet[] = self) do
      res = Enum.drop_while self.rules, fn(r) ->
        case r.year do
          :min .. :max -> false
          :min .. x when y < x -> false
          :min .. ^y -> !r.contains(first)
          x .. :max when y > x -> false
          ^y .. :max -> !r.contains(first)
          x1 .. x2 when y > x1 and y < x2 -> false
          Range[first: ^y] -> !r.contains(first)
          Range[last: ^y] -> !r.contains(first)
          ^y -> !r.contains(first)
          _ -> true
        end
      end

      Enum.take_while res, fn(r) ->
        r.contains(last)
      end
    end
  end

  parse_range = fn
    "min", "max" ->
      :min.. :max
    "min", x ->
      :min..binary_to_integer(x)
    x, "max" ->
      binary_to_integer(x).. :max
    x, "only" ->
      binary_to_integer(x)
    x1, x2 ->
      binary_to_integer(x1)..binary_to_integer(x2)
  end

  last_weekday_of_the_month = fn(year, month, weekday) ->
    Enum.find :calendar.last_day_of_the_month(year, month)..1, fn(day) ->
      :calendar.day_of_the_week(year, month, day) == weekday
    end
  end

  [first_weekday_of_the_month_ge, first_weekday_of_the_month_le] = fn() ->
    first_weekday_of_the_month = fn(year, month, weekday, fun) ->
      last = :calendar.last_day_of_the_month(year, month)
      first = Enum.find 1.. :calendar.last_day_of_the_month(year, month), fn(d) ->
        :calendar.day_of_the_week(year, month, d) == weekday
      end

      Enum.find (Enum.map 0..trunc((last - first) / 7), fn(x) ->
        x * 7 + first
      end), fun
    end

    [fn(year, month, weekday, day) ->
      first_weekday_of_the_month.(year, month, weekday, &1 >= day)
    end,
    fn(year, month, weekday, day) ->
      first_weekday_of_the_month.(year, month, weekday, &1 <= day)
    end]
  end.()

  parse_weekday = fn
    "Mon" -> 1
    "Tue" -> 2
    "Wed" -> 3
    "Thu" -> 4
    "Fri" -> 5
    "Sat" -> 6
    "Sun" -> 7
  end

  parse_day = fn
    <<?l, ?a, ?s, ?t, day :: binary>> -> { :last, parse_weekday.(day) }

    <<d, a, y, sig, ?=, int :: binary>> ->
      int = binary_to_integer(int)
      day = parse_weekday.(<<d, a, y>>)
      case sig do
        ?> -> { :>=, day, int }
        ?< -> { :<=, day, int }
      end

    x -> binary_to_integer(x)
  end

  parse_time = fn
    "-" -> {{0, 0, 0},:u}
    ts ->
      {flag, rtime} =
        case String.last(ts) do
          f when f == "s" or f == "w" or f == "u" -> { binary_to_atom(f), String.slice(ts, 0, String.length(ts) - 1) }
          f when f == "g" or f == "z" -> { :u, String.slice(ts, 0, String.length(ts) - 1) }
          <<f>> when f >= ?0 and f <= ?9 -> { :u, ts }
        end
      time =
        case String.split(rtime, ":") do
          [hs] -> { binary_to_integer(hs), 0, 0 }
          [hs, ms] -> { binary_to_integer(hs), binary_to_integer(ms), 0 }
          [hs, ms, ss] -> { binary_to_integer(hs), binary_to_integer(ms), binary_to_integer(ss) }
        end
      {time, flag}
  end

  parse_time_val = fn str ->
    {time, _} = parse_time.(str)
    time
  end

 parse_month = fn
    "Jan" -> 1
    "Feb" -> 2
    "Mar" -> 3
    "Apr" -> 4
    "May" -> 5
    "Jun" -> 6
    "Jul" -> 7
    "Aug" -> 8
    "Sep" -> 9
    "Oct" -> 10
    "Nov" -> 11
    "Dec" -> 12
  end

  parse_until = fn
    [] -> nil
    [ys] -> {{binary_to_integer(ys),1,1},{0,0,0}}
    [ys,ms] -> {{binary_to_integer(ys),parse_month.(ms),1},{0,0,0}}
    [ys,ms,ds] -> {{binary_to_integer(ys),parse_month.(ms),parse_day.(ds)},{0,0,0}}
    [ys,ms,ds,ts] -> {{binary_to_integer(ys),parse_month.(ms),parse_day.(ds)},parse_time_val.(ts)}
  end

  to_offset = fn
    {h,m,s} -> (h * 60 + m) * 60 + s
  end

  parse_rule = fn
    [from, to, _, month, day, time, save, letters] ->
      year = parse_range.(from, to)
      month = parse_month.(month)
      day =
        case year do
          Range[] -> parse_day.(day)
          _ ->
            case parse_day.(day) do
              {:last,d} -> last_weekday_of_the_month.(year, month, d)
              {:>=,d,i} -> first_weekday_of_the_month_ge.(year, month, d, i)
              {:<=,d,i} -> first_weekday_of_the_month_le.(year, month, d, i)
              d -> d
            end
        end
      time = parse_time_val.(time)
      save = to_offset.(parse_time_val.(save))
      Rule.new year: year, month: month, day: day, time: time, save: save, letters: letters
  end

  parse_zone = fn
    [gmtoff,rules,format|until] ->
      offset = to_offset.(parse_time_val.(gmtoff))
      rules =
        case rules do
          "-" -> nil
          r -> r
        end

      until =
        case parse_until.(until) do
          {{y,m,day},t} ->
            day =
              case day do
                {:last,d} -> last_weekday_of_the_month.(y, m, d)
                {:>=,d,i} -> first_weekday_of_the_month_ge.(y, m, d, i)
                {:<=,d,i} -> first_weekday_of_the_month_le.(y, m, d, i)
                d -> d
              end
              {{y,m,day},t}
            nil -> nil
        end

      Zone.new offset: offset, rules: rules, format: format, until: until
  end

  append = fn
    key, subkey, src, el ->
      ssrc = Keyword.get src, key, []
      v = :proplists.get_value(subkey, ssrc, [])
      ssrc = [{subkey, [el|v]}| :proplists.delete(subkey, ssrc)]

      Keyword.put src, key, ssrc
  end

  db_dir = Path.expand(Path.join(["..", "..", "priv", "tzdata"]), __FILE__)

  records = Enum.reduce File.ls!(db_dir), [], fn(file, records) ->
    case Regex.match? %r/\./, file do
      true -> records
      false ->
        file = Path.join(db_dir, file)
        case File.regular?(file) do
          true ->
            Enum.reduce File.iterator!(file), records, fn(line, records) ->
              line = String.strip(String.strip(String.strip(Regex.replace(%r/#.*$/, line, "")), ?\n), ?\t)
              case String.length(line) > 0 do
                true ->
                  case Regex.split(%r/[\t ]+/, line) do
                    ["Rule",name|data] ->
                      append.(:rules, name, Keyword.put(records, :prev, nil), parse_rule.(data))

                    ["Link",name,link] ->
                      Keyword.put Keyword.put(records, :prev, nil), :links, [{name, link}| Keyword.get records, :links, []]

                    ["Leap"|_] ->
                      Keyword.put records, :prev, nil

                    ["Zone",name|data] ->
                      append.(:zones, name, Keyword.put(records, :prev, name), parse_zone.(data))
                    data ->
                      case records[:prev] do
                        nil -> Keyword.put records, :prev, nil
                        name -> append.(:zones, name, records, parse_zone.(data))
                      end
                  end
                false -> records
              end
            end
          false -> records
        end
    end
  end

  zones = Enum.map records[:zones], fn({key, value}) ->
      { key, { Timezone, key, Enum.reverse value } }
  end

  zones = Enum.reduce records[:links], zones, fn({from,to}, z) ->
    [{ to, z[from] } | z]
  end
  zones = HashDict.new zones

  def zones(), do: unquote Macro.escape zones
  def get(name), do: HashDict.get zones, name

  def rulesets() do
    unquote Macro.escape HashDict.new Enum.map records[:rules], fn({key, value}) ->
      { key, Timezone.RuleSet.new name: key, rules: Enum.reverse value }
    end
  end
  def ruleset(name), do: HashDict.get rulesets, name

end
