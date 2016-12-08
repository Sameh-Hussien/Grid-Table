#ifndef FACTORYCREATOR_H
#define FACTORYCREATOR_H
#include <string>
#include <map>
#include <memory>
#include <utility>
#include <iostream>
#include <stdexcept>

using namespace std;

template<typename BaseT, typename ... ARGs>
class Factory {
public:

    static Factory* instance() {
        static Factory inst;
        return &inst;
    }

    template<typename T>
    void reg(const string& name) {
        m_stock[name].reset(new Creator<T>);
        /*std::cout << m_stock.size();
        if (!(m_stock.find(name) == m_stock.end())) {
            std::cout << "found";
        }*/
    }

    BaseT* create(const string& name, ARGs&&...args) {
        //std::cout << m_stock.size();
        if ((m_stock.find(name) == m_stock.end())) {
            throw std::invalid_argument("error: invalid type \""+name+"\"\r\n");
        }
        return m_stock[name]->create(forward<ARGs>(args)...);
    }

    struct ICreator {
        virtual BaseT* create(ARGs&&...) = 0;

    };

    template<typename T>
    struct Creator : public ICreator {

        virtual BaseT* create(ARGs&&...args) {
            return new T(forward<ARGs>(args)...);
        }
    };
    std::map<string, std::unique_ptr<ICreator>> m_stock;
};

template<typename BaseT, typename T, typename...ARGs>
class Register {
public:

    Register(const string& name) {
        auto instance = Factory<BaseT, ARGs...>::instance();
        instance->template reg<T>(name);
    }
};

#endif /* FACTORYCREATOR_H */