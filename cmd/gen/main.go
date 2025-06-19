package main

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

type bObject struct {
	ID                string   `json:"id"`
	Type              string   `json:"type,omitempty"`
	Name              string   `json:"name,omitempty"`
	PreferredUsername string   `json:"preferredUsername,omitempty"`
	Content           string   `json:"content,omitempty"`
	URL               string   `json:"url,omitempty"`
	Object            *bObject `json:"object,omitempty"`
	Actor             *bObject `json:"actor,omitempty"`
}

var (
	base, _     = os.Getwd()
	objectTypes = []string{
		"Article",
		"Audio",
		"Document",
		"Event",
		"Image",
		"Note",
		"Page",
		"Place",
		"Profile",
		"Relationship",
		"Tombstone",
		"Video",
	}
	actorTypes = []string{
		"Application",
		"Group",
		"Organization",
		"Person",
		"Service",
	}

	activityTypes = []string{
		"Accept",
		"Add",
		"Announce",
		"Block",
		"Create",
		"Delete",
		"Dislike",
		"Flag",
		"Follow",
		"Ignore",
		"Invite",
		"Join",
		"Leave",
		"Like",
		"Listen",
		"Move",
		"Offer",
		"Reject",
		"Read",
		"Remove",
		"TentativeReject",
		"TentativeAccept",
		"Undo",
		"Update",
		"View",
	}

	names = []string{
		"Alice", "Bob", "Jane", "John", "Diana", "Elephant", "Diogenes", "Charlie", "Anders",
		"Ross", "Hank", "Jill", "Llewyn", "Osgir", "Thor", "Robin", "Peter", "Pan", "Stephen",
		"Zod", "Una", "El", "Nermal",
	}

	content = []string{
		"Lorem ipsum dolor sit amet, consectetur adipiscing elit.",
		"Cras elementum leo lectus, at condimentum sapien ornare ac.",
		"Quisque lorem elit, scelerisque nec commodo ac, maximus nec neque.",
		"In porttitor augue ac dolor viverra, eget fringilla augue tincidunt.",
		"Suspendisse potenti.",
		"Cras pulvinar gravida purus, id tincidunt sem vestibulum vel.",
		"Nam ut odio id risus laoreet scelerisque.",
		"Donec finibus sem vitae nisi ultricies dictum.",
		"Nulla nec enim in velit iaculis elementum sit amet a nibh.",
		"Nulla semper aliquet tincidunt.",
		"Vestibulum eleifend eros metus, eget congue mauris molestie ut.",
		"Nullam turpis turpis, malesuada non accumsan vitae, congue ac justo.",
		"Cras sit amet porta libero.",
		"Maecenas vestibulum odio at pellentesque gravida.",
		"In velit libero, ultrices nec quam at, lacinia congue purus.",
		"Proin ac ligula ac magna sodales commodo.",
		"Sed est elit, facilisis eu malesuada non, mattis nec risus.",
		"Suspendisse blandit tempor faucibus.",
		"Sed metus dolor, vehicula ut cursus luctus, pellentesque a sapien.",
		"Maecenas dapibus, mi quis elementum imperdiet, ipsum dolor molestie est, sit amet finibus nisi nunc et orci.",
		"Nulla facilisi.",
		"Sed efficitur purus nec tellus ultricies condimentum.",
		"Quisque id mi aliquet, pellentesque diam eu, euismod nisl.",
		"Ut lacinia ligula a bibendum pulvinar.",
		"Sed quam ante, feugiat id lobortis eget, dictum a ante.",
		"Quisque ac dolor tellus.",
		"Phasellus blandit odio in pretium pretium.",
		"Phasellus sit amet aliquam quam.",
		"Mauris at erat accumsan, aliquet purus et, egestas elit.",
		"Pellentesque varius at dui nec rutrum.",
		"Nunc sollicitudin ut orci vel bibendum.",
		"In at vulputate est.",
		"Sed quis turpis ut sapien venenatis cursus.",
		"Donec accumsan pulvinar risus, eu ultrices est volutpat lobortis.",
		"Donec quis tempus eros, ut bibendum nibh.",
		"Vivamus eget maximus quam, non dignissim sapien.",
		"Fusce sit amet eros in lacus porta vehicula.",
		"Sed ultrices nisl arcu, et sollicitudin diam imperdiet vitae.",
		"Curabitur tincidunt mattis ornare.",
		"Phasellus molestie neque magna, sit amet rhoncus ex tempor et.",
		"Aliquam gravida gravida urna ac ornare.",
		"Cras pharetra libero.",
	}

	generators = randomFns{randomActor, randomActivity, randomObject}
)

type (
	randomFn  func(string) bObject
	randomFns []randomFn
)

func randomFromSlice[T ~string](list []T) T {
	i := rand.Intn(len(list))
	return list[i]
}

func randomActivity(u string) bObject {
	a := bObject{}
	a.ID = "https://" + u
	host := filepath.Dir(a.ID)
	a.Type = randomFromSlice(activityTypes)
	act := randomActor(host)
	a.Actor = &act
	ob := randomObject(filepath.Join(a.ID, "object"))
	a.Object = &ob
	return a
}

func randomActor(u string) bObject {
	ob := bObject{}

	pu := randomName()
	u = strings.ReplaceAll(u, filepath.Base(u), pu)
	ob.ID = "https://" + u

	ob.Type = randomFromSlice(actorTypes)
	ob.PreferredUsername = pu
	return ob
}

func randomObject(u string) bObject {
	ob := bObject{}
	ob.ID = "https://" + u
	ob.Type = randomFromSlice(objectTypes)
	if ob.Type != "Tombstone" {
		if ob.Type == "Page" {
			ob.URL = ob.ID
		} else {
			ob.Name = randomTitle()
			ob.Content = randomContent()
		}
	}
	return ob
}

func (g randomFns) run(u string) bObject {
	i := rand.Intn(len(g))
	fn := g[i]

	return fn(u)
}

func randomName() string {
	return randomFromSlice(names)
}

func randomTitle() string {
	cl := rand.Intn(len(content))
	return content[cl]
}

func randomContent() string {
	maxLines := len(content)
	lineCount := rand.Intn(maxLines)
	ss := strings.Builder{}
	for range lineCount {
		cl := rand.Intn(maxLines)
		ss.WriteString(content[cl])
		ss.WriteRune('\n')
	}
	return ss.String()
}

func main() {
	mockPath := filepath.Clean(filepath.Join(base, "mocks"))
	fi, err := os.Stat(mockPath)
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "%+s for path %s", err, mockPath)
		os.Exit(1)
		return
	}
	if !fi.IsDir() {
		_, _ = fmt.Fprintf(os.Stderr, "%s is not a directory", mockPath)
		os.Exit(1)
		return
	}
	for i := range 100 {
		url := filepath.Join("example.com", "inbox", strconv.Itoa(i))
		itemPath := filepath.Join(mockPath, url)
		err = os.Mkdir(itemPath, 0700)
		if err != nil && !os.IsExist(err) {
			_, _ = fmt.Fprintf(os.Stderr, "%+s, unable to create\n", err)
			continue
		}
		f, err := os.OpenFile(filepath.Join(itemPath, "__raw"), os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
		if err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "%+s when opening file\n", err)
			if !os.IsExist(err) {
				continue
			}
		}

		ob := generators.run(url)
		if err = json.NewEncoder(f).Encode(&ob); err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "%+s encode error: %s\n", err, itemPath)
		}
	}
}
